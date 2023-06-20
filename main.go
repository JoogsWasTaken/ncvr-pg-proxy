package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq"
)

func main() {
	var connStr string
	var pgKeyQuery string
	var maxGroupSize int

	flag.StringVar(&connStr, "p", "", "url to postgres db")
	flag.StringVar(&pgKeyQuery, "q", "", "postgres key query")
	flag.IntVar(&maxGroupSize, "l", 5, "max group size")
	flag.Parse()

	db, err := sql.Open("postgres", connStr)

	if err != nil {
		log.Fatal(err)
	}

	hashCount := make(map[string]int)
	hashBlacklist := make(map[string]struct{})

	defer db.Close()

	log.Println("starting query")
	start := time.Now()

	// for the love of god do not do this unless you control exactly what pgKeyQuery
	// can be, otherwise this is a perfect sqli vector!
	rows, err := db.Query(fmt.Sprintf("select md5(%[1]s) from ncvr_plausible", pgKeyQuery))

	if err != nil {
		log.Fatal(err)
	}

	log.Println("query finished, fetching results")

	defer rows.Close()
	var hash string

	for rows.Next() {
		// read hash from result column
		if err := rows.Scan(&hash); err != nil {
			log.Fatal(err)
		}

		// check if the hash has already been blacklisted, aka exceeded maxGroupSize
		if _, ok := hashBlacklist[hash]; ok {
			continue
		}

		// if the hash has exceeded maxGroupSize, add it to the blacklist
		if v, ok := hashCount[hash]; ok && v == maxGroupSize {
			hashBlacklist[hash] = struct{}{}
			continue
		}

		hashCount[hash]++
	}

	if err = rows.Err(); err != nil {
		log.Fatal(err)
	}

	groupSizeToCount := make(map[int]int)

	for _, v := range hashCount {
		groupSizeToCount[v]++
	}

	duration := time.Since(start)
	log.Printf("query finished, took %f\n", duration.Seconds())

	for i := 1; i <= maxGroupSize; i++ {
		fmt.Printf("%d\t%d\n", i, groupSizeToCount[i])
	}
}
