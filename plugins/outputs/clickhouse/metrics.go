package clickhouse

import (
	"math"
	"sort"
	"time"

	"github.com/influxdata/telegraf"
)

type (
	Pair struct {
		k string
		v interface{}
	}
	// a metric of clickhouse
	clickhouseMetric struct {
		Date    string    `json:"date" db:"date"`
		Name    string    `json:"name" db:"name"`
		Tags    []Pair    `json:"tags" db:"tags"`
		Fields  []Pair    `json:"val" db:"val"`
		Ts      time.Time `json:"ts" db:"ts"`
		Updated time.Time `json:"updated" db:"updated"`
		Columns map[string]interface{}
	}
)

func newClickhouseMetric(metric telegraf.Metric, keyPriority []string) *clickhouseMetric {
	var clickhouseMetric clickhouseMetric
	var tags []Pair
	var fields []Pair
	columns := make(map[string]interface{})
	keyPriorityMap := make(map[string]int)
	for idx, key := range keyPriority {
		keyPriorityMap[key] = idx
	}

	clickhouseMetric.Name = metric.Name()
	clickhouseMetric.Updated = time.Now()
	clickhouseMetric.Ts = metric.Time()
	columns["updated"] = clickhouseMetric.Updated
	columns["ts"] = clickhouseMetric.Ts

	for _, tag := range metric.TagList() {
		tags = append(tags, Pair{tag.Key, tag.Value})
		columns[tag.Key] = tag.Value
	}
	sort.SliceStable(tags, func(i, j int) bool {
		return getOrDefault(&keyPriorityMap, tags[i].k, math.MaxInt64) <
			getOrDefault(&keyPriorityMap, tags[j].k, math.MaxInt64)
	})
	clickhouseMetric.Tags = tags

	for _, field := range metric.FieldList() {
		tmp_field := convertField(field.Value)
		if tmp_field == nil {
			fields = append(fields, Pair{field.Key, float64(0)})
		} else {
			fields = append(fields, Pair{field.Key, tmp_field})
		}
		columns[field.Key] = tmp_field
	}
	clickhouseMetric.Fields = fields
	clickhouseMetric.Columns = columns
	return &clickhouseMetric
}
