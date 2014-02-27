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

package metric

import (
	"container/heap"
	"time"

	clientmodel "github.com/prometheus/client_golang/model"
)

var (
	// firstSupertime is the smallest valid supertime that may be seeked to.
	firstSupertime = []byte{0, 0, 0, 0, 0, 0, 0, 0}
	// lastSupertime is the largest valid supertime that may be seeked to.
	lastSupertime = []byte{127, 255, 255, 255, 255, 255, 255, 255}
)

// ViewRequestBuilder represents the summation of all datastore queries that
// shall be performed to extract values.  Each operation mutates the state of
// the builder.
type ViewRequestBuilder interface {
	GetMetricAtTime(fingerprint *clientmodel.Fingerprint, time clientmodel.Timestamp)
	GetMetricAtInterval(fingerprint *clientmodel.Fingerprint, from, through clientmodel.Timestamp, interval time.Duration)
	GetMetricRange(fingerprint *clientmodel.Fingerprint, from, through clientmodel.Timestamp)
	ScanJobs() scanJobs
}

// viewRequestBuilder contains the various requests for data.
type viewRequestBuilder struct {
	operations scanJobs
}

// NewViewRequestBuilder furnishes a ViewRequestBuilder for remarking what types
// of queries to perform.
func NewViewRequestBuilder() *viewRequestBuilder {
	return &viewRequestBuilder{
		operations: scanJobs{},
	}
}

var getValuesAtTimes = newValueAtTimeList(10 * 1024)

// GetMetricAtTime gets for the given Fingerprint either the value at that time
// if there is an match or the one or two values adjacent thereto.
func (v *viewRequestBuilder) GetMetricAtTime(fp *clientmodel.Fingerprint, time clientmodel.Timestamp) {
	op, _ := getValuesAtTimes.Get()
	op.time = time
	heap.Push(&v.operations, &scanJob{fingerprint: *fp, operation: op})
}

var getValuesAtIntervals = newValueAtIntervalList(10 * 1024)

// GetMetricAtInterval gets for the given Fingerprint either the value at that
// interval from From through Through if there is an match or the one or two
// values adjacent for each point.
func (v *viewRequestBuilder) GetMetricAtInterval(fp *clientmodel.Fingerprint, from, through clientmodel.Timestamp, interval time.Duration) {
	op, _ := getValuesAtIntervals.Get()
	op.from = from
	op.through = through
	op.interval = interval
	heap.Push(&v.operations, &scanJob{fingerprint: *fp, operation: op})
}

var getValuesAlongRanges = newValueAlongRangeList(10 * 1024)

// GetMetricRange gets for the given Fingerprint the values that occur
// inclusively from From through Through.
func (v *viewRequestBuilder) GetMetricRange(fp *clientmodel.Fingerprint, from, through clientmodel.Timestamp) {
	op, _ := getValuesAlongRanges.Get()
	op.from = from
	op.through = through
	heap.Push(&v.operations, &scanJob{fingerprint: *fp, operation: op})
}

var getValuesAtIntervalAlongRanges = newValueAtIntervalAlongRangeList(10 * 1024)

// GetMetricRangeAtInterval gets value ranges at intervals for the given
// Fingerprint:
//
//   |----|       |----|       |----|       |----|
//   ^    ^            ^       ^    ^            ^
//   |    \------------/       \----/            |
//  from     interval       rangeDuration     through
func (v *viewRequestBuilder) GetMetricRangeAtInterval(fp *clientmodel.Fingerprint, from, through clientmodel.Timestamp, interval, rangeDuration time.Duration) {
	op, _ := getValuesAtIntervalAlongRanges.Get()
	op.rangeFrom = from
	op.rangeThrough = from.Add(rangeDuration)
	op.rangeDuration = rangeDuration
	op.interval = interval
	op.through = through
	heap.Push(&v.operations, &scanJob{fingerprint: *fp, operation: op})
}

// ScanJobs emits the scans that will occur in the data store.  This effectively
// resets the ViewRequestBuilder back to a pristine state.
func (v *viewRequestBuilder) ScanJobs() scanJobs {
	operationsToEmit := v.operations
	v.operations = scanJobs{}
	return operationsToEmit
}

type view struct {
	*memorySeriesStorage
}

func (v view) appendSamples(fingerprint *clientmodel.Fingerprint, samples Values) {
	v.memorySeriesStorage.appendSamplesWithoutIndexing(fingerprint, samples)
}

func newView() view {
	return view{NewMemorySeriesStorage(MemorySeriesOptions{})}
}

func giveBackOp(op interface{}) bool {
	switch v := op.(type) {
	case *getValuesAtTimeOp:
		return getValuesAtTimes.Give(v)
	case *getValuesAtIntervalOp:
		return getValuesAtIntervals.Give(v)
	case *getValuesAlongRangeOp:
		return getValuesAlongRanges.Give(v)
	case *getValueRangeAtIntervalOp:
		return getValuesAtIntervalAlongRanges.Give(v)
	default:
		panic("unrecognized operation")
	}
}
