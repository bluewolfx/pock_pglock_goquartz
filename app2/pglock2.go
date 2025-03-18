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

	for {
		ok, err := lock.Lock(ctx)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("lock.Lock()==%v\n", ok)

		if !ok {
			log.Println("Lock in use by another process, waiting...")
			time.Sleep(2 * time.Second)
			continue
		}
		log.Println("successfully acquired lock in app2!")

		// do some work with the lock...

		rows, err := db.Query("SELECT * FROM pglock;")
		if err != nil {
			log.Fatal(err)
		}
		defer rows.Close()

		cols, err := rows.Columns()
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("columns: %s", cols)

		break
	}

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
