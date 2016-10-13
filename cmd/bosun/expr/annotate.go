package expr

import (
	"fmt"
	"strings"
	"time"

	"bosun.org/cmd/bosun/expr/parse"
	"bosun.org/models"
	"bosun.org/opentsdb"
	"github.com/MiniProfiler/go/miniprofiler"
	"github.com/bosun-monitor/annotate"
	"github.com/kylebrandt/boolq"
)

var Annotate = map[string]parse.Func{
	// Funcs for querying elastic
	"ancounts": {
		Args:   []models.FuncType{models.TypeString, models.TypeString, models.TypeString},
		Return: models.TypeSeriesSet,
		Tags:   tagFirst,
		F:      AnCounts,
	},
	"antable": {
		Args:   []models.FuncType{models.TypeString, models.TypeString, models.TypeString, models.TypeString},
		Return: models.TypeTable,
		F:      AnTable,
	},
}

func procDuration(e *State, startDuration, endDuration string) (*time.Time, *time.Time, error) {
	start, err := opentsdb.ParseDuration(startDuration)
	if err != nil {
		return nil, nil, err
	}
	var end opentsdb.Duration
	if endDuration != "" {
		end, err = opentsdb.ParseDuration(endDuration)
		if err != nil {
			return nil, nil, err
		}
	}
	st := e.now.Add(time.Duration(-start))
	en := e.now.Add(time.Duration(-end))
	return &st, &en, nil
}

func getAndFilterAnnotations(e *State, start, end *time.Time, filter string) (annotate.Annotations, error) {
	annotations, err := e.AnnotateContext.GetAnnotations(start, end, "", "", "", "", "", "", "")
	if err != nil {
		return nil, err
	}
	filteredAnnotations := annotate.Annotations{}
	for _, a := range annotations {
		if filter == "" {
			filteredAnnotations = append(filteredAnnotations, a)
			continue
		}
		match, err := boolq.AskExpr(filter, a)
		if err != nil {
			return nil, err
		}
		if match {
			filteredAnnotations = append(filteredAnnotations, a)
		}
	}
	return filteredAnnotations, nil
}

func AnCounts(e *State, T miniprofiler.Timer, filter, startDuration, endDuration string) (r *Results, err error) {
	start, end, err := procDuration(e, startDuration, endDuration)
	if err != nil {
		return nil, err
	}
	filteredAnnotations, err := getAndFilterAnnotations(e, start, end, filter)
	if err != nil {
		return nil, err
	}
	series := make(Series)
	for i, a := range filteredAnnotations {
		inBounds := (a.StartDate.Time.After(*start) || a.StartDate.Time == *start) && (a.EndDate.Time.Before(*end) || a.EndDate.Time == *end)
		entirelyOutOfBounds := a.StartDate.Before(*start) && a.EndDate.Time.After(*end)
		if inBounds || entirelyOutOfBounds {
			series[time.Unix(int64(i), 0).UTC()] = 1
			continue
		}
		aDuration := a.EndDate.Time.Sub(a.StartDate.Time)
		if aDuration == 0 {
			series[time.Unix(int64(i), 0).UTC()] = 1
			continue
		}
		if a.StartDate.Time.Before(*start) {
			durationBeforeStart := start.Sub(a.StartDate.Time)
			percentBeforeStart := float64(durationBeforeStart) / float64(aDuration)
			series[time.Unix(int64(i), 0).UTC()] = percentBeforeStart
			continue
		}
		if a.EndDate.Time.After(*end) {
			durationAfterEnd := a.EndDate.Time.Sub(*end)
			percentAfterEnd := float64(durationAfterEnd) / float64(aDuration)
			series[time.Unix(int64(i), 0).UTC()] = percentAfterEnd
		}
	}
	return &Results{
		Results: []*Result{
			{Value: series},
		},
	}, nil
}

// AnTable returns a table response (meant for Grafana) of matching annotations based on the requested fields
func AnTable(e *State, T miniprofiler.Timer, filter, fieldsCSV, startDuration, endDuration string) (r *Results, err error) {
	start, end, err := procDuration(e, startDuration, endDuration)
	if err != nil {
		return nil, err
	}
	columns := strings.Split(fieldsCSV, ",")
	columnLen := len(columns)
	if columnLen == 0 {
		return nil, fmt.Errorf("must specify at least one column")
	}
	columnIndex := make(map[string]int, columnLen)
	for i, v := range columns {
		// switch is so we fail before fetching annotations
		switch v {
		case "start", "end", "owner", "user", "host", "category", "url", "message", "duration":
			// Pass
		default:
			return nil, fmt.Errorf("%v is not a valid column, must be start, end, owner, user, host, category, url, or message", v)
		}
		columnIndex[v] = i
	}
	filteredAnnotations, err := getAndFilterAnnotations(e, start, end, filter)
	if err != nil {
		return nil, err
	}
	t := Table{Columns: columns}
	for _, a := range filteredAnnotations {
		row := make([]interface{}, columnLen)
		for _, c := range columns {
			switch c {
			case "start":
				row[columnIndex["start"]] = a.StartDate
			case "end":
				row[columnIndex["end"]] = a.EndDate
			case "owner":
				row[columnIndex["owner"]] = a.Owner
			case "user":
				row[columnIndex["user"]] = a.CreationUser
			case "host":
				row[columnIndex["host"]] = a.Host
			case "category":
				row[columnIndex["category"]] = a.Category
			case "url":
				row[columnIndex["url"]] = a.Url
			case "message":
				row[columnIndex["message"]] = a.Message
			case "duration":
				row[columnIndex["duration"]] = a.EndDate.Sub(a.StartDate.Time).String()
			}
		}
		t.Rows = append(t.Rows, row)
	}
	return &Results{
		Results: []*Result{
			{Value: t},
		},
	}, nil
}
