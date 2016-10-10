package expr

import (
	"time"

	"bosun.org/cmd/bosun/expr/parse"
	"bosun.org/models"
	"bosun.org/opentsdb"
	"github.com/MiniProfiler/go/miniprofiler"
)

var Annotate = map[string]parse.Func{
	// Funcs for querying elastic
	"ancount": {
		Args:   []models.FuncType{models.TypeString, models.TypeString, models.TypeString},
		Return: models.TypeSeriesSet,
		Tags:   elasticTagQuery,
		F:      AnCount,
	},
}

func AnCount(e *State, T miniprofiler.Timer, filter, startDuration, endDuration string) (r *Results, err error) {
	start, err := opentsdb.ParseDuration(startDuration)
	if err != nil {
		return nil, err
	}
	var end opentsdb.Duration
	if endDuration != "" {
		end, err = opentsdb.ParseDuration(endDuration)
		if err != nil {
			return nil, err
		}
	}
	st := e.now.Add(time.Duration(-start))
	en := e.now.Add(time.Duration(-end))
	annotations, err := e.AnnotateContext.GetAnnotations(&st, &en, "", "", "", "", "", "", "")
	if err != nil {
		return nil, err
	}
	return &Results{
		Results: []*Result{
			{Value: Scalar(float64(len(annotations)))},
		},
	}, nil
}
