package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/snownd/tdquery"
)

type Data struct {
	Ts    int64
	Value float64
}

func handleQueryResult(r *tdquery.QueryResult, err error) {
	if err != nil {
		panic(err)
	}
	if r.Code != 0 {
		panic(r.Message)
	}
}

const db = "tdquery_example"
const stable = "sensors"

// normally you may not want to insert data with rest api
func runInserts(c *tdquery.Client) {
	for i := 0; i < 10; i++ {
		insertSQL := fmt.Sprintf(`INSERT INTO %s.%s USING %s.%s TAGS(?) (ts, value) VALUES (?, ?)`, db, "s_"+strconv.Itoa(i), db, stable)
		now := time.Now()
		for j := 0; j < 100; j++ {
			r, err := c.Query(context.Background(), insertSQL, strconv.Itoa(i%3+1001), now.Add(time.Duration(j)*(-time.Second)), float64(i*j))
			handleQueryResult(r, err)
		}
	}
}

func main() {
	client := tdquery.NewClient(
		tdquery.WithBrokers([]string{"localhost"}),
		tdquery.WithBasicAuth("root", "taosdata"),
	)
	if err := client.Connect(context.Background()); err != nil {
		panic(err)
	}
	defer client.Close(context.Background())
	r, err := client.Query(context.Background(), "CREATE DATABASE IF NOT EXISTS "+db)
	handleQueryResult(r, err)
	fmt.Printf("create database result : %+v\n", r)
	r, err = client.Query(context.Background(), fmt.Sprintf("CREATE STABLE IF NOT EXISTS %s.%s (ts TIMESTAMP, value DOUBLE) TAGS (city_code INT)", db, stable))
	handleQueryResult(r, err)
	fmt.Printf("create stable result : %+v\n", r)
	runInserts(client)

	ret := make([]Data, 0)
	qb := client.NewSelectQueryBuilder().UseDatabase(db)
	err = qb.SelectColumnWithAlias("MAX(value)", "value").
		FromSTable(stable).
		WithTimeScope(time.Now().Add(-1*time.Hour), time.Now()).
		Where(tdquery.Equals("city_code", 1002)).
		Interval(tdquery.NewInterval("3s")).
		Desc().
		Limit(3).
		GetResult(context.TODO(), &ret)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%+v\n", ret)
}
