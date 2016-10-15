package expr

import (
	"fmt"
	"sort"
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
	"andurations": {
		Args:   []models.FuncType{models.TypeString, models.TypeString, models.TypeString},
		Return: models.TypeSeriesSet,
		Tags:   tagFirst,
		F:      AnDurations,
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
	sort.Sort(sort.Reverse(annotate.AnnotationsByStartID(filteredAnnotations)))
	return filteredAnnotations, nil
}

func AnDurations(e *State, T miniprofiler.Timer, filter, startDuration, endDuration string) (r *Results, err error) {
	reqStart, reqEnd, err := procDuration(e, startDuration, endDuration)
	if err != nil {
		return nil, err
	}
	filteredAnnotations, err := getAndFilterAnnotations(e, reqStart, reqEnd, filter)
	if err != nil {
		return nil, err
	}
	series := make(Series)
	for i, a := range filteredAnnotations {
		aStart := a.StartDate.Time
		aEnd := a.EndDate.Time
		inBounds := (aStart.After(*reqStart) || aStart == *reqStart) && (aEnd.Before(*reqEnd) || aEnd == *reqEnd)
		entirelyOutOfBounds := aStart.Before(*reqStart) && aEnd.After(*reqEnd)
		aDuration := aEnd.Sub(aStart)
		if inBounds {
			// time has no meaning here, so we just make the key an index since we don't have an array type
			series[time.Unix(int64(i), 0).UTC()] = aDuration.Seconds()
		}
		if entirelyOutOfBounds {
			// Duration is equal to that of the full request
			series[time.Unix(int64(i), 0).UTC()] = reqEnd.Sub(*reqStart).Seconds()
		}
		if aDuration == 0 {
			// This would mean an out of bounds. Should never be here, but if we don't return an error in the case that we do end up here then we might panic on divide by zero later in the code
			return nil, fmt.Errorf("unexpected annotation with 0 duration outside of request bounds (please file an issue)")
		}
		if aStart.Before(*reqStart) {
			aDurationAfterReqStart := aEnd.Sub(*reqStart)
			series[time.Unix(int64(i), 0).UTC()] = aDurationAfterReqStart.Seconds()
			continue
		}
		if aEnd.After(*reqEnd) {
			aDurationBeforeReqEnd := reqEnd.Sub(aStart)
			series[time.Unix(int64(i), 0).UTC()] = aDurationBeforeReqEnd.Seconds()
		}
	}
	return &Results{
		Results: []*Result{
			{Value: series},
		},
	}, nil
	return nil, nil
}

func AnCounts(e *State, T miniprofiler.Timer, filter, startDuration, endDuration string) (r *Results, err error) {
	reqStart, reqEnd, err := procDuration(e, startDuration, endDuration)
	if err != nil {
		return nil, err
	}
	filteredAnnotations, err := getAndFilterAnnotations(e, reqStart, reqEnd, filter)
	if err != nil {
		return nil, err
	}
	series := make(Series)
	for i, a := range filteredAnnotations {
		aStart := a.StartDate.Time
		aEnd := a.EndDate.Time
		inBounds := (aStart.After(*reqStart) || aStart == *reqStart) && (aEnd.Before(*reqEnd) || aEnd == *reqEnd)
		entirelyOutOfBounds := aStart.Before(*reqStart) && aEnd.After(*reqEnd)
		if inBounds || entirelyOutOfBounds {
			// time has no meaning here, so we just make the key an index since we don't have an array type
			series[time.Unix(int64(i), 0).UTC()] = 1
			continue
		}
		aDuration := aEnd.Sub(aStart)
		if aDuration == 0 {
			// This would mean an out of bounds. Should never be here, but if we don't return an error in the case that we do end up here then we might panic on divide by zero later in the code
			return nil, fmt.Errorf("unexpected annotation with 0 duration outside of request bounds (please file an issue)")
		}
		if aStart.Before(*reqStart) {
			aDurationAfterReqStart := aEnd.Sub(*reqStart)
			percentBeforeStart := float64(aDurationAfterReqStart) / float64(aDuration)
			series[time.Unix(int64(i), 0).UTC()] = percentBeforeStart
			continue
		}
		if aEnd.After(*reqEnd) {
			aDurationBeforeReqEnd := reqEnd.Sub(aStart)
			percentAfterEnd := float64(aDurationBeforeReqEnd) / float64(aDuration)
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
				d := a.EndDate.Sub(a.StartDate.Time)
				// Format Time in a way that can be lexically sorted
				row[columnIndex["duration"]] = hhmmss(d)
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

// hhmmss formats a duration into HHH:MM:SS (Hours, Minutes, Seconds) so it can be lexically sorted
// up to 999 hours
func hhmmss(d time.Duration) string {
	hours := int64(d.Hours())
	minutes := int64((d - time.Duration(time.Duration(hours)*time.Hour)).Minutes())
	seconds := int64((d - time.Duration(time.Duration(minutes)*time.Minute)).Seconds())
	return fmt.Sprintf("%03d:%02d:%02d", hours, minutes, seconds)
}
