// Copyright 2013 Prometheus Team
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/golang/glog"

	clientmodel "github.com/prometheus/client_golang/model"

	"github.com/prometheus/prometheus/rules"
	"github.com/prometheus/prometheus/rules/ast"
	"github.com/prometheus/prometheus/stats"
	"github.com/prometheus/prometheus/web/httputils"
)

// Enables cross-site script calls.
func setAccessControlHeaders(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Headers", "Accept, Authorization, Content-Type, Origin")
	w.Header().Set("Access-Control-Allow-Methods", "GET")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Expose-Headers", "Date")
}

// Query handles the /api/query endpoint.
func (serv MetricsService) Query(w http.ResponseWriter, r *http.Request) {
	setAccessControlHeaders(w)

	params := httputils.GetQueryParams(r)
	expr := params.Get("expr")
	asText := params.Get("asText")

	var format ast.OutputFormat
	// BUG(julius): Use Content-Type negotiation.
	if asText == "" {
		format = ast.JSON
		w.Header().Set("Content-Type", "application/json")
	} else {
		format = ast.Text
		w.Header().Set("Content-Type", "text/plain")
	}

	exprNode, err := rules.LoadExprFromString(expr)
	if err != nil {
		fmt.Fprint(w, ast.ErrorToJSON(err))
		return
	}

	timestamp := clientmodel.TimestampFromTime(serv.time.Now())

	queryStats := stats.NewTimerGroup()
	result := ast.EvalToString(exprNode, timestamp, format, serv.Storage, queryStats)
	glog.V(1).Infof("Instant query: %s\nQuery stats:\n%s\n", expr, queryStats)
	fmt.Fprint(w, result)
}

// QueryRange handles the /api/query_range endpoint.
func (serv MetricsService) QueryRange(w http.ResponseWriter, r *http.Request) {
	setAccessControlHeaders(w)
	w.Header().Set("Content-Type", "application/json")

	params := httputils.GetQueryParams(r)
	expr := params.Get("expr")

	// Input times and durations are in seconds and get converted to nanoseconds.
	endFloat, _ := strconv.ParseFloat(params.Get("end"), 64)
	durationFloat, _ := strconv.ParseFloat(params.Get("range"), 64)
	stepFloat, _ := strconv.ParseFloat(params.Get("step"), 64)
	nanosPerSecond := int64(time.Second / time.Nanosecond)
	end := int64(endFloat) * nanosPerSecond
	duration := int64(durationFloat) * nanosPerSecond
	step := int64(stepFloat) * nanosPerSecond

	exprNode, err := rules.LoadExprFromString(expr)
	if err != nil {
		fmt.Fprint(w, ast.ErrorToJSON(err))
		return
	}
	if exprNode.Type() != ast.VectorType {
		fmt.Fprint(w, ast.ErrorToJSON(errors.New("expression does not evaluate to vector type")))
		return
	}

	if end == 0 {
		end = clientmodel.Now().UnixNano()
	}

	if step <= 0 {
		step = nanosPerSecond
	}

	if end-duration < 0 {
		duration = end
	}

	// For safety, limit the number of returned points per timeseries.
	// This is sufficient for 60s resolution for a week or 1h resolution for a year.
	if duration/step > 11000 {
		fmt.Fprint(w, ast.ErrorToJSON(errors.New("exceeded maximum resolution of 11,000 points per timeseries. Try decreasing the query resolution (?step=XX)")))
		return
	}

	// Align the start to step "tick" boundary.
	end -= end % step

	queryStats := stats.NewTimerGroup()

	evalTimer := queryStats.GetTimer(stats.TotalEvalTime).Start()
	matrix, err := ast.EvalVectorRange(
		exprNode.(ast.VectorNode),
		clientmodel.TimestampFromUnixNano(end-duration),
		clientmodel.TimestampFromUnixNano(end),
		time.Duration(step),
		serv.Storage,
		queryStats)
	if err != nil {
		fmt.Fprint(w, ast.ErrorToJSON(err))
		return
	}
	evalTimer.Stop()

	sortTimer := queryStats.GetTimer(stats.ResultSortTime).Start()
	sort.Sort(matrix)
	sortTimer.Stop()

	jsonTimer := queryStats.GetTimer(stats.JSONEncodeTime).Start()
	result := ast.TypedValueToJSON(matrix, "matrix")
	jsonTimer.Stop()

	glog.V(1).Infof("Range query: %s\nQuery stats:\n%s\n", expr, queryStats)
	fmt.Fprint(w, result)
}

// Metrics handles the /api/metrics endpoint.
func (serv MetricsService) Metrics(w http.ResponseWriter, r *http.Request) {
	setAccessControlHeaders(w)

	metricNames := serv.Storage.GetLabelValuesForLabelName(clientmodel.MetricNameLabel)
	sort.Sort(metricNames)
	resultBytes, err := json.Marshal(metricNames)
	if err != nil {
		glog.Error("Error marshalling metric names: ", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(resultBytes)
}
