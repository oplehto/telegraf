package clickhouse

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/influxdata/telegraf"
	"github.com/juju/errors"
	"github.com/kshvakov/clickhouse"
)

type ClickhouseClient struct {
	// DBI example: tcp://host1:9000?username=user&password=qwerty&database=clicks&read_timeout=10&write_timeout=20&alt_hosts=host2:9000,host3:9000

	DBI          string
	Addr         string   `toml:"addr"`
	Port         int64    `toml:"port"`
	User         string   `toml:"user"`
	Password     string   `toml:"password"`
	Database     string   `toml:"database"`
	WriteTimeout int64    `toml:"write_timeout"`
	KeyPriority  []string `toml:"key_priority"`
	ExcludeKeys  []string `toml:"exclude_key"`
	CreateTable  bool     `toml:"create_table"`
	Debug        bool     `toml:"debug"`
	WhiteList    []string `toml:"white_list"`

	WhiteListAll bool
	WhiteListSet map[string]bool
	db           *sql.DB
	loopCnt      int
}

func newClickhouse() *ClickhouseClient {
	return &ClickhouseClient{}
}

func (c *ClickhouseClient) Connect() error {
	var err error

	c.DBI = fmt.Sprintf("tcp://%s:%d?username=%s&password=%s&write_timeout=%d&debug=%t",
		c.Addr,
		c.Port,
		c.User,
		c.Password,
		c.WriteTimeout,
		c.Debug,
	)

	if c.Debug {
		log.Println("DBI=", c.DBI)
	}

	c.WhiteListSet = make(map[string]bool)
	c.WhiteListAll = false
	for _, i := range c.WhiteList {
		c.WhiteListSet[i] = true
		if i == "*" {
			c.WhiteListAll = true
		}
	}

	c.db, err = sql.Open("clickhouse", c.DBI)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *ClickhouseClient) Close() error {
	return nil
}

func (c *ClickhouseClient) Description() string {
	return "Telegraf Output Plugin for Clickhouse"
}

func (c *ClickhouseClient) SampleConfig() string {
	return `
telegraf.conf
[[outputs.clickhouse]]
    user = "default"
    password = ""
    addr = 127.0.0.1
    port = 9000
    database = "telegraf"
	write_timeout = 10
	debug = true
	# For automatic schema generation
	create_table = true
	key_priority = ["dc","bu","cls","env","job_owner","sr","host"]
	exclude_key = ["path"]
`
}
func (c *ClickhouseClient) CreateTables(grouped_metrics map[string][]*clickhouseMetric) (err error) {
	err = nil

	for name, metrics := range grouped_metrics {
		var metric *clickhouseMetric
		if len(metrics) > 0 {
			metric = metrics[0]
		} else {
			continue
		}

		var tag_type_list, field_type_list, partition_key_list []string

		for _, pair := range metric.Tags {
			tag_type := fmt.Sprintf(`%s LowCardinality(%s)`, pair.k, convertTypeName(pair.v))
			tag_type_list = append(tag_type_list, tag_type)
			if !contains(c.ExcludeKeys, pair.k) {
				partition_key_list = append(partition_key_list, pair.k)
			}
		}
		for _, pair := range metric.Fields {
			tag_type := fmt.Sprintf(`%s %s`, pair.k, convertTypeName(pair.v))
			field_type_list = append(field_type_list, tag_type)
		}
		stmtCreateTable := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.%s(
	date Date DEFAULT toDate(ts),
	%s,
	%s,
	ts DateTime,
	updated DateTime DEFAULT now()
) ENGINE=MergeTree(date,( %s ,ts),8192)
`,
			c.Database, name,
			strings.Join(tag_type_list, ",\n"),
			strings.Join(field_type_list, ",\n"),
			strings.Join(partition_key_list, ", "))

		if c.Debug {
			log.Println("Create Table :", stmtCreateTable)
		}

		_, err = c.db.Exec(stmtCreateTable)
		if err != nil {
			if c.Debug {
				log.Fatal(err.Error())
			}
			return errors.Trace(err)
		}
	}
	return err
}

func (c *ClickhouseClient) GetColumns(table string) (map[string]string, error) {
	fields := make(map[string]string)
	describeTableStmt := fmt.Sprintf("DESCRIBE TABLE telegraf.%s", table)
	rows, err := c.db.Query(describeTableStmt)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var name, type_name, default_type, default_expression, comment, codec_expression, ttl_expression string
		if err := rows.Scan(&name, &type_name, &default_type, &default_expression, &comment, &codec_expression, &ttl_expression); err != nil {
			log.Fatal(err)
		}
		if name != "date" {
			fields[name] = type_name
		}
	}
	return fields, err
}

func (c *ClickhouseClient) InsertData(grouped_metrics map[string][]*clickhouseMetric) (err error) {
	var wg sync.WaitGroup
	c.loopCnt++
	// ("abc", 3, ",") -> "abc,abc,abc"
	repeat := func(s string, cnt int, sep string) string {
		var list_str []string
		for i := 0; i < cnt; i++ {
			list_str = append(list_str, s)
		}
		return strings.Join(list_str, sep)
	}

	wg.Add(len(grouped_metrics))
	for name, metrics := range grouped_metrics {
		go func(name string, metrics []*clickhouseMetric) {
			defer wg.Done()
			number_drop := len(metrics)
			err = nil

			Tx, err := c.db.Begin()
			if c.Debug {
				log.Println("Starting Transaction.")
			}
			if err != nil {
				if c.Debug {
					log.Fatal(err.Error())
				}
				log.Fatal(errors.Trace(err))
			}

			// Prepare stmt
			var column_names []string
			columns, err := c.GetColumns(name)
			for k := range columns {
				column_names = append(column_names, k)
			}

			stmtInsertData := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)",
				c.Database, name,
				strings.Join(column_names, ","),
				repeat("?", len(column_names), ", "))
			Stmt, err := Tx.Prepare(stmtInsertData)
			if err != nil {
				if c.Debug {
					log.Println(err.Error())
				}
				log.Fatal(errors.Trace(err))
			}
			defer Stmt.Close()

			for _, metric := range metrics {
				vals := make([]interface{}, 0)

				for _, columnName := range column_names {
					columnType := columns[columnName]
					val, ok := metric.Columns[columnName]
					if ok {
						vals = append(vals, val)
					} else {
						vals = append(vals, typeDefaultVal(columnType))
					}
				}
				if c.Debug {
					log.Println(name, ": (", strings.Join(printList(vals), ", "), ")")
				}
				if _, err := Stmt.Exec(vals...); err != nil {
					if c.Debug {
						fmt.Println(err.Error())
					}
				}
			}
			// commit transaction.
			if err := Tx.Commit(); err != nil {
				log.Println("loop ", c.loopCnt, " drop ", number_drop, " , ", name)
				log.Fatal(errors.Trace(err))
			}

			if c.Debug {
				log.Println("Transaction Commit")
			}
		}(name, metrics)
	}
	wg.Wait()
	return nil
}

func (c *ClickhouseClient) Write(metrics []telegraf.Metric) (err error) {
	err = nil
	grouped_metrics := make(map[string][]*clickhouseMetric)

	for _, metric := range metrics {
		_, ok := c.WhiteListSet[metric.Name()]
		if c.WhiteListAll || ok {
			var tmpClickhouseMetrics *clickhouseMetric
			tmpClickhouseMetrics = newClickhouseMetric(metric, c.KeyPriority)
			grouped_metrics[tmpClickhouseMetrics.Name] = append(grouped_metrics[tmpClickhouseMetrics.Name], tmpClickhouseMetrics)
		}
	}

	if err = c.db.Ping(); err != nil {
		if c.Debug {
			if exception, ok := err.(*clickhouse.Exception); ok {
				log.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
			} else {
				log.Println(err)
			}
		}
		return errors.Trace(err)
	}

	// create database
	stmtCreateDatabase := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", c.Database)
	if c.Debug {
		log.Println("Create Database: ", stmtCreateDatabase)
	}
	_, err = c.db.Exec(stmtCreateDatabase)
	if err != nil {
		if c.Debug {
			log.Println(err.Error())
		}
		return errors.Trace(err)
	}

	// create table
	if c.CreateTable {
		err = c.CreateTables(grouped_metrics)
		if err != nil {
			if c.Debug {
				log.Println(err.Error())
			}
			return errors.Trace(err)
		}
	}

	// write data
	err = c.InsertData(grouped_metrics)
	if err != nil {
		if c.Debug {
			log.Println(err.Error())
		}
		return errors.Trace(err)
	}

	return err
}
