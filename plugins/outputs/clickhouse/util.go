package clickhouse

import (
	"fmt"
	"time"
)

func getOrDefault(m *map[string]int, key string, val int) int {
	if v, ok := (*m)[key]; ok {
		return v
	} else {
		return val
	}
}

func contains(slice []string, item string) bool {
	set := make(map[string]struct{}, len(slice))
	for _, s := range slice {
		set[s] = struct{}{}
	}

	_, ok := set[item]
	return ok
}

// convert field to a supported type or nil if unconvertible
func convertField(v interface{}) interface{} {
	switch v := v.(type) {
	case float64:
		return v
	case int64:
		return float64(v)
	case bool:
		if v {
			return float64(1)
		} else {
			return float64(0)
		}
	case int:
		return float64(v)
	case uint:
		return float64(v)
	case uint64:
		return float64(v)
	case int32:
		return float64(v)
	case int16:
		return float64(v)
	case int8:
		return float64(v)
	case uint32:
		return float64(v)
	case uint16:
		return float64(v)
	case uint8:
		return float64(v)
	case float32:
		return float64(v)
	case string:
		return v
	default:
		return nil
	}
}
func convertTypeName(v interface{}) string {
	switch v.(type) {
	case float64:
		return "Float64"
	case string:
		return "String"
	default:
		return "String"
	}
}

func typeDefaultVal(type_name string) interface{} {
	switch type_name {
	case "String":
		return ""
	case "LowCardinality(String)":
		return ""
	case "Float64":
		return 0.0
	case "Int32":
		return 0
	default:
		return nil
	}
}

func printList(l []interface{}) []string {
	var res []string
	for _, val := range l {
		switch val.(type) {
		case string:
			res = append(res, fmt.Sprintf("'%s'", val))
		case time.Time:
			res = append(res,
				fmt.Sprintf("%d",
					val.(time.Time).Unix()))

		default:
			res = append(res, fmt.Sprintf("%f", val))
		}
	}
	return res
}
