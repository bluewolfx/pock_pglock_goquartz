package main

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/reugn/go-quartz/job"
	"github.com/reugn/go-quartz/logger"
	"github.com/reugn/go-quartz/quartz"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	slogLogger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	scheduler, _ := quartz.NewStdScheduler(quartz.WithLogger(logger.NewSlogLogger(ctx, slogLogger)))
	scheduler.Start(ctx)

	dbJob := job.NewFunctionJob(func(_ context.Context) (int, error) {
		if db, err := newDBQuartz(); err == nil {
			defer db.Close()
			rows, err := db.QueryContext(context.Background(), "SELECT * FROM pglock;")
			if err != nil {
				slogLogger.Info("[Q1] DB query failed", "error", err)
			} else {
				defer rows.Close()
				slogLogger.Info("[Q1] DB is reachable!")
				cols, err := rows.Columns()
				if err != nil {
					slogLogger.Error("[Q1] Failed to get columns", "error", err)
					return 0, err
				}

				fmt.Printf("columns: %v\n", cols)
			}
		} else {
			slogLogger.Info("[Q1] Connection error", "error", err)
		}
		return 0, nil
	})

	_ = scheduler.ScheduleJob(
		quartz.NewJobDetail(dbJob, quartz.NewJobKey("dbJobQ1")),
		quartz.NewSimpleTrigger(5*time.Second),
	)

	// Wait for termination signal instead of stopping immediately
	slogLogger.Info("[Q1] Scheduler running. Press Ctrl+C to stop.")
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	// Stop gracefully when signal received
	slogLogger.Info("[Q1] Shutting down...")
	scheduler.Stop()
	scheduler.Wait(ctx)
}

func newDBQuartz() (*sql.DB, error) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://teste:teste@localhost:15432/postgres?sslmode=disable"
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	return db, db.Ping()
}
