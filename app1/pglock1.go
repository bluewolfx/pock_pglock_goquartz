package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/allisson/go-pglock/v3"
	_ "github.com/lib/pq"
	"log"
	"os"
	"time"
)

func main() {
	RunPgLock()
}

func RunPgLock() {
	db, err := newDB()
	if err != nil {
		log.Fatal(err)
	}
	defer closeDB(db)

	ctx := context.Background()
	id := int64(1)
	lock, err := pglock.NewLock(ctx, id, db)
	if err != nil {
		log.Fatal(err)
	}

	ok, err := lock.Lock(ctx)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("lock1.Lock()==%v\n", ok)
	log.Println("successfully acquired lock in app1!")

	fmt.Println("Using lock ID:", id)

	fmt.Println("App1 holding lock for 45 seconds...")
	time.Sleep(45 * time.Second)

}

func newDB() (*sql.DB, error) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		// Fallback if env var isn't loaded
		dsn = "postgres://teste:teste@localhost:5432/postgres?sslmode=disable"
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	return db, db.Ping()
}

func closeDB(db *sql.DB) {
	if err := db.Close(); err != nil {
		log.Fatal(err)
	}
}
