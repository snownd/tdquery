## tdquery

A simple go client for [TDengine](https://github.com/taosdata/TDengine)


## Installation

```bash
go get githhub.com/snownd/tdquery
```

## Usage

```go

	client := tdquery.NewClient(
		tdquery.WithBrokers([]string{"localhost"}),
		tdquery.WithPort(6041),
		tdquery.WithBasicAuth("root", "taosdata"),
    // tdquery.WithUrlDatabase() use this when TDengine version is greater than 2.2.0.0
	)
 	if err := client.Connect(context.Background()); err != nil {
		panic(err)
	}
  	ret := make([]Data, 0)
	qb := client.NewSelectQueryBuilder().UseDatabase(db)
	err = qb.SelectColumnWithAlias("MAX(value)", "value").
		FromSTable(stable).
		WithTimeScope(time.Now().Add(-1*time.Hour), time.Now()).
		Where(tdquery.Equals("city_code", 1002)).
		Interval(tdquery.NewInterval("3s")).
		Desc().
		Limit(3).
    // GetRaw() will have much better performance than GetResult()
		GetResult(context.TODO(), &ret)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%+v\n", ret)
```

You can check [example](./examples/query/main.go) for more usage.

---
## TODO

- [] Add test cases, and use github Action do tests
- [] Add more examples
- [] Add Insert Builder
- [] Add more `Condition` for TDengine SQL aggregation functions
- [] Add Support for JOIN
- [] Add Support for UNION ALL
- [] HTTP keepalive
- [] Taosd token authentication


