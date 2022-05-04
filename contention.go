package main

import (
	"context"
	"flag"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach-go/v2/crdb/crdbpgx"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/log/logrusadapter"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/sirupsen/logrus"
)

func main() {

	var conn string
	var par int
	var n int
	var test string
	var size int
	flag.StringVar(&conn, "pgurl", "", "Connection port to CRDB cluster.")
	flag.IntVar(&par, "par", 100, "number of parallel workers")
	flag.IntVar(&n, "n", 10000, "number of rows to insert")
	flag.IntVar(&size, "size", 10000, "size of rows")
	flag.StringVar(&test, "test", "withTxn", "one of "+strings.Join(testNames, ", "))
	flag.Parse()
	ctx := context.Background()
	cfg, err := pgxpool.ParseConfig(conn)
	if err != nil {
		logrus.WithError(err).Fatal("failed to parse test connection")
	}
	cfg.ConnConfig.LogLevel = pgx.LogLevelError
	cfg.ConnConfig.Logger = logrusadapter.NewLogger(logrus.New())
	cfg.MaxConns = 128
	client, err := pgxpool.ConnectConfig(ctx, cfg)
	if err != nil {
		logrus.WithError(err).Fatal("failed to connect to crdb")
	}

	for _, stmt := range []string{
		`CREATE TABLE IF NOT EXISTS kv
			(
				key VARCHAR(512) NOT NULL PRIMARY KEY,
				value BLOB NOT NULL
			);`,
		`CREATE TABLE IF NOT EXISTS test
			(
				id UUID DEFAULT gen_random_uuid() NOT NULL PRIMARY KEY
			);`,
		`SET CLUSTER SETTING kv.rangefeed.enabled = true;`,
		`SET CLUSTER SETTING changefeed.experimental_poll_interval = '0.2s';`,
	} {
		if _, err := client.Exec(ctx, stmt); err != nil {
			logrus.WithError(err).Fatal("error initializing the database")
		}
	}
	op, ok := tests[test]
	if !ok {
		logrus.Fatalf("unknown workload %s", test)
	}
	if err := do(ctx, n, par, op(client), test, size); err != nil {
		logrus.WithError(err).Fatal("failed to interact with CRDB")
	}
}

type operationFunc func(ctx context.Context, size int) error

var testNames = func() (names []string) {
	for name := range tests {
		names = append(names, name)
	}
	return names
}()

var tests = map[string]func(client *pgxpool.Pool) operationFunc{
	// raw inserts, no contention since we have no SELECT
	"raw": func(client *pgxpool.Pool) operationFunc {
		return func(ctx context.Context, size int) error {
			var id uuid.UUID
			return client.QueryRow(ctx, `INSERT INTO kv (key, value) VALUES ($1, $2) RETURNING key;`, uuid.New().String(), strings.Repeat("x", size)).Scan(&id)
		}
	},
	// withSelectAfter has a SELECT afterwards to read the row. This doesn't
	// generalize in the face of updates.
	"withSelectAfter": func(client *pgxpool.Pool) operationFunc {
		return func(ctx context.Context, size int) error {
			var id uuid.UUID
			err := client.QueryRow(ctx, `INSERT INTO kv (key, value) VALUES ($1, $2) RETURNING key;`, uuid.New().String(), strings.Repeat("x", size)).Scan(&id)
			if err != nil {
				return err
			}
			var hybridLogicalTimestamp apd.Decimal
			return client.QueryRow(ctx, `SELECT crdb_internal_mvcc_timestamp FROM kv WHERE key=$1;`, id).Scan(&hybridLogicalTimestamp)
		}
	},
	// withFixedTimestamp has a SELECT in the transaction to observe the logical timestamp of the transaction.
	"withFixedTimestamp": func(client *pgxpool.Pool) operationFunc {
		var hybridLogicalTimestamp apd.Decimal
		return func(ctx context.Context, size int) error {
			return crdbpgx.ExecuteTx(ctx, client, pgx.TxOptions{}, func(tx pgx.Tx) error {
				if _, err := tx.Exec(ctx, `INSERT INTO kv (key, value) VALUES ($1, $2);`, uuid.New().String(), strings.Repeat("x", size)); err != nil {
					return err
				}

				return tx.QueryRow(ctx, `SELECT cluster_logical_timestamp()`).Scan(&hybridLogicalTimestamp)
			})
		}
	},
	// withTxn has a transaction on the insert, all of a sudden we see contention on the SELECT
	"withSeparateRow": func(client *pgxpool.Pool) operationFunc {
		return func(ctx context.Context, size int) error {
			var id uuid.UUID
			if err := crdbpgx.ExecuteTx(ctx, client, pgx.TxOptions{}, func(tx pgx.Tx) error {
				if err := tx.QueryRow(ctx, `INSERT INTO test (id) VALUES (DEFAULT) RETURNING id;`).Scan(&id); err != nil {
					return err
				}
				_, err := tx.Exec(ctx, `INSERT INTO kv (key, value) VALUES ($1, $2);`, uuid.New().String(), strings.Repeat("x", size))
				return err
			}); err != nil {
				return err
			}

			var hybridLogicalTimestamp apd.Decimal
			return client.QueryRow(ctx, `SELECT crdb_internal_mvcc_timestamp FROM test WHERE id=$1;`, id).Scan(&hybridLogicalTimestamp)
		}
	},
	"withToken": func(client *pgxpool.Pool) operationFunc {
		return func(ctx context.Context, size int) error {
			var hybridLogicalTimestamp apd.Decimal
			if err := crdbpgx.ExecuteTx(ctx, client, pgx.TxOptions{}, func(tx pgx.Tx) error {
				if _, err := tx.Exec(ctx, `INSERT INTO kv (key, value) VALUES ($1, $2);`, uuid.New().String(), strings.Repeat("x", size)); err != nil {
					return err
				}

				return tx.QueryRow(ctx, `SELECT crdb_internal.commit_with_causality_token()`).Scan(&hybridLogicalTimestamp)
			}); err != nil {
				return err
			}
			return nil
		}
	},
}

func do(
	ctx context.Context,
	iterations, parallelism int,
	operation operationFunc,
	msg string,
	size int,
) error {
	var durations []time.Duration
	sink := make(chan time.Duration, parallelism)
	wg := sync.WaitGroup{}
	start := time.Now()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case duration, ok := <-sink:
				if !ok {
					return
				}
				durations = append(durations, duration)

				if len(durations)%(iterations/10) == 0 {
					summarize(durations, "processing metrics", size, parallelism, time.Since(start))
				}
			}
		}
	}()
	if err := workers(ctx, iterations, parallelism, func(ctx context.Context) error {
		before := time.Now()
		err := operation(ctx, size)
		duration := time.Since(before)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case sink <- duration:
		}
		return err
	}); err != nil {
		return err
	}
	close(sink)
	wg.Wait()
	summarize(durations, msg, size, parallelism, time.Since(start))
	return nil
}

func workers(
	ctx context.Context, iterations, parallelism int, operation func(context.Context) error,
) error {
	wg := &sync.WaitGroup{}
	operations := make(chan struct{})
	for i := 0; i < parallelism; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case _, ok := <-operations:
					if !ok {
						return
					}
					if err := operation(ctx); err != nil {
						logrus.WithError(err).Error("failed to interact with the API server")
					}
				}
			}
		}()
	}

	start := time.Now()
	for i := 0; i < iterations; i++ {
		start = display(i, iterations, start)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case operations <- struct{}{}:
		}
	}
	close(operations)
	wg.Wait()
	return nil
}

func display(i, max int, start time.Time) time.Time {
	if (i+1)%(max/10) == 0 {
		logrus.Infof("progress: %d/%d (%.0f%%); %s per ", i+1, max, 100*(float64(i+1)/float64(max)), time.Since(start)/time.Duration(max/10))
		return time.Now()
	}
	return start
}

func summarize(durations []time.Duration, msg string, size, par int, dur time.Duration) {
	var all time.Duration
	for _, duration := range durations {
		all += duration
	}
	logrus.WithFields(logrus.Fields{
		"count":      len(durations),
		"mean":       all / time.Duration(len(durations)),
		"size":       size,
		"workers":    par,
		"throughput": float64(len(durations)) / dur.Seconds(),
	}).Info(msg)
}
