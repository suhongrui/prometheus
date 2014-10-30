// Copyright 2014 Prometheus Team
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

package local

import (
	"fmt"
	"math/rand"
	"testing"
	"testing/quick"
	"time"

	clientmodel "github.com/prometheus/client_golang/model"
	"github.com/prometheus/prometheus/storage/metric"
	"github.com/prometheus/prometheus/utility/test"
)

func TestGetFingerprintsForLabelMatchers(t *testing.T) {

}

// TestLoop is just a smoke test for the loop method, if we can switch it on and
// off without disaster.
func TestLoop(t *testing.T) {
	samples := make(clientmodel.Samples, 1000)
	for i := range samples {
		samples[i] = &clientmodel.Sample{
			Timestamp: clientmodel.Timestamp(2 * i),
			Value:     clientmodel.SampleValue(float64(i) * 0.2),
		}
	}
	directory := test.NewTemporaryDirectory("test_storage", t)
	defer directory.Close()
	o := &MemorySeriesStorageOptions{
		MemoryEvictionInterval:     100 * time.Millisecond,
		MemoryRetentionPeriod:      time.Hour,
		PersistencePurgeInterval:   150 * time.Millisecond,
		PersistenceRetentionPeriod: 24 * 7 * time.Hour,
		PersistenceStoragePath:     directory.Path(),
		CheckpointInterval:         250 * time.Millisecond,
	}
	storage, err := NewMemorySeriesStorage(o)
	if err != nil {
		t.Fatalf("Error creating storage: %s", err)
	}
	storage.Start()
	storage.AppendSamples(samples)
	time.Sleep(time.Second)
	storage.Stop()
}

func TestChunk(t *testing.T) {
	samples := make(clientmodel.Samples, 500000)
	for i := range samples {
		samples[i] = &clientmodel.Sample{
			Timestamp: clientmodel.Timestamp(i),
			Value:     clientmodel.SampleValue(float64(i) * 0.2),
		}
	}
	s, closer := NewTestStorage(t)
	defer closer.Close()

	s.AppendSamples(samples)

	for m := range s.(*memorySeriesStorage).fpToSeries.iter() {
		for i, v := range m.series.values() {
			if samples[i].Timestamp != v.Timestamp {
				t.Fatalf("%d. Got %v; want %v", i, v.Timestamp, samples[i].Timestamp)
			}
			if samples[i].Value != v.Value {
				t.Fatalf("%d. Got %v; want %v", i, v.Value, samples[i].Value)
			}
		}
	}
}

func TestGetValueAtTime(t *testing.T) {
	samples := make(clientmodel.Samples, 1000)
	for i := range samples {
		samples[i] = &clientmodel.Sample{
			Timestamp: clientmodel.Timestamp(2 * i),
			Value:     clientmodel.SampleValue(float64(i) * 0.2),
		}
	}
	s, closer := NewTestStorage(t)
	defer closer.Close()

	s.AppendSamples(samples)

	fp := clientmodel.Metric{}.Fingerprint()

	it := s.NewIterator(fp)

	// #1 Exactly on a sample.
	for i, expected := range samples {
		actual := it.GetValueAtTime(expected.Timestamp)

		if len(actual) != 1 {
			t.Fatalf("1.%d. Expected exactly one result, got %d.", i, len(actual))
		}
		if expected.Timestamp != actual[0].Timestamp {
			t.Errorf("1.%d. Got %v; want %v", i, actual[0].Timestamp, expected.Timestamp)
		}
		if expected.Value != actual[0].Value {
			t.Errorf("1.%d. Got %v; want %v", i, actual[0].Value, expected.Value)
		}
	}

	// #2 Between samples.
	for i, expected1 := range samples {
		if i == len(samples)-1 {
			continue
		}
		expected2 := samples[i+1]
		actual := it.GetValueAtTime(expected1.Timestamp + 1)

		if len(actual) != 2 {
			t.Fatalf("2.%d. Expected exactly 2 results, got %d.", i, len(actual))
		}
		if expected1.Timestamp != actual[0].Timestamp {
			t.Errorf("2.%d. Got %v; want %v", i, actual[0].Timestamp, expected1.Timestamp)
		}
		if expected1.Value != actual[0].Value {
			t.Errorf("2.%d. Got %v; want %v", i, actual[0].Value, expected1.Value)
		}
		if expected2.Timestamp != actual[1].Timestamp {
			t.Errorf("2.%d. Got %v; want %v", i, actual[1].Timestamp, expected1.Timestamp)
		}
		if expected2.Value != actual[1].Value {
			t.Errorf("2.%d. Got %v; want %v", i, actual[1].Value, expected1.Value)
		}
	}

	// #3 Corner cases: Just before the first sample, just after the last.
	expected := samples[0]
	actual := it.GetValueAtTime(expected.Timestamp - 1)
	if len(actual) != 1 {
		t.Fatalf("3.1. Expected exactly one result, got %d.", len(actual))
	}
	if expected.Timestamp != actual[0].Timestamp {
		t.Errorf("3.1. Got %v; want %v", actual[0].Timestamp, expected.Timestamp)
	}
	if expected.Value != actual[0].Value {
		t.Errorf("3.1. Got %v; want %v", actual[0].Value, expected.Value)
	}
	expected = samples[len(samples)-1]
	actual = it.GetValueAtTime(expected.Timestamp + 1)
	if len(actual) != 1 {
		t.Fatalf("3.2. Expected exactly one result, got %d.", len(actual))
	}
	if expected.Timestamp != actual[0].Timestamp {
		t.Errorf("3.2. Got %v; want %v", actual[0].Timestamp, expected.Timestamp)
	}
	if expected.Value != actual[0].Value {
		t.Errorf("3.2. Got %v; want %v", actual[0].Value, expected.Value)
	}
}

func TestGetRangeValues(t *testing.T) {
	samples := make(clientmodel.Samples, 1000)
	for i := range samples {
		samples[i] = &clientmodel.Sample{
			Timestamp: clientmodel.Timestamp(2 * i),
			Value:     clientmodel.SampleValue(float64(i) * 0.2),
		}
	}
	s, closer := NewTestStorage(t)
	defer closer.Close()

	s.AppendSamples(samples)

	fp := clientmodel.Metric{}.Fingerprint()

	it := s.NewIterator(fp)

	// #1 Zero length interval at sample.
	for i, expected := range samples {
		actual := it.GetRangeValues(metric.Interval{
			OldestInclusive: expected.Timestamp,
			NewestInclusive: expected.Timestamp,
		})

		if len(actual) != 1 {
			t.Fatalf("1.%d. Expected exactly one result, got %d.", i, len(actual))
		}
		if expected.Timestamp != actual[0].Timestamp {
			t.Errorf("1.%d. Got %v; want %v.", i, actual[0].Timestamp, expected.Timestamp)
		}
		if expected.Value != actual[0].Value {
			t.Errorf("1.%d. Got %v; want %v.", i, actual[0].Value, expected.Value)
		}
	}

	// #2 Zero length interval off sample.
	for i, expected := range samples {
		actual := it.GetRangeValues(metric.Interval{
			OldestInclusive: expected.Timestamp + 1,
			NewestInclusive: expected.Timestamp + 1,
		})

		if len(actual) != 0 {
			t.Fatalf("2.%d. Expected no result, got %d.", i, len(actual))
		}
	}

	// #3 2sec interval around sample.
	for i, expected := range samples {
		actual := it.GetRangeValues(metric.Interval{
			OldestInclusive: expected.Timestamp - 1,
			NewestInclusive: expected.Timestamp + 1,
		})

		if len(actual) != 1 {
			t.Fatalf("3.%d. Expected exactly one result, got %d.", i, len(actual))
		}
		if expected.Timestamp != actual[0].Timestamp {
			t.Errorf("3.%d. Got %v; want %v.", i, actual[0].Timestamp, expected.Timestamp)
		}
		if expected.Value != actual[0].Value {
			t.Errorf("3.%d. Got %v; want %v.", i, actual[0].Value, expected.Value)
		}
	}

	// #4 2sec interval sample to sample.
	for i, expected1 := range samples {
		if i == len(samples)-1 {
			continue
		}
		expected2 := samples[i+1]
		actual := it.GetRangeValues(metric.Interval{
			OldestInclusive: expected1.Timestamp,
			NewestInclusive: expected1.Timestamp + 2,
		})

		if len(actual) != 2 {
			t.Fatalf("4.%d. Expected exactly 2 results, got %d.", i, len(actual))
		}
		if expected1.Timestamp != actual[0].Timestamp {
			t.Errorf("4.%d. Got %v for 1st result; want %v.", i, actual[0].Timestamp, expected1.Timestamp)
		}
		if expected1.Value != actual[0].Value {
			t.Errorf("4.%d. Got %v for 1st result; want %v.", i, actual[0].Value, expected1.Value)
		}
		if expected2.Timestamp != actual[1].Timestamp {
			t.Errorf("4.%d. Got %v for 2nd result; want %v.", i, actual[1].Timestamp, expected2.Timestamp)
		}
		if expected2.Value != actual[1].Value {
			t.Errorf("4.%d. Got %v for 2nd result; want %v.", i, actual[1].Value, expected2.Value)
		}
	}

	// #5 corner cases: Interval ends at first sample, interval starts
	// at last sample, interval entirely before/after samples.
	expected := samples[0]
	actual := it.GetRangeValues(metric.Interval{
		OldestInclusive: expected.Timestamp - 2,
		NewestInclusive: expected.Timestamp,
	})
	if len(actual) != 1 {
		t.Fatalf("5.1. Expected exactly one result, got %d.", len(actual))
	}
	if expected.Timestamp != actual[0].Timestamp {
		t.Errorf("5.1. Got %v; want %v.", actual[0].Timestamp, expected.Timestamp)
	}
	if expected.Value != actual[0].Value {
		t.Errorf("5.1. Got %v; want %v.", actual[0].Value, expected.Value)
	}
	expected = samples[len(samples)-1]
	actual = it.GetRangeValues(metric.Interval{
		OldestInclusive: expected.Timestamp,
		NewestInclusive: expected.Timestamp + 2,
	})
	if len(actual) != 1 {
		t.Fatalf("5.2. Expected exactly one result, got %d.", len(actual))
	}
	if expected.Timestamp != actual[0].Timestamp {
		t.Errorf("5.2. Got %v; want %v.", actual[0].Timestamp, expected.Timestamp)
	}
	if expected.Value != actual[0].Value {
		t.Errorf("5.2. Got %v; want %v.", actual[0].Value, expected.Value)
	}
	firstSample := samples[0]
	actual = it.GetRangeValues(metric.Interval{
		OldestInclusive: firstSample.Timestamp - 4,
		NewestInclusive: firstSample.Timestamp - 2,
	})
	if len(actual) != 0 {
		t.Fatalf("5.3. Expected no results, got %d.", len(actual))
	}
	lastSample := samples[len(samples)-1]
	actual = it.GetRangeValues(metric.Interval{
		OldestInclusive: lastSample.Timestamp + 2,
		NewestInclusive: lastSample.Timestamp + 4,
	})
	if len(actual) != 0 {
		t.Fatalf("5.3. Expected no results, got %d.", len(actual))
	}
}

func TestEvictAndPurgeSeries(t *testing.T) {
	samples := make(clientmodel.Samples, 1000)
	for i := range samples {
		samples[i] = &clientmodel.Sample{
			Timestamp: clientmodel.Timestamp(2 * i),
			Value:     clientmodel.SampleValue(float64(i) * 0.2),
		}
	}
	s, closer := NewTestStorage(t)
	defer closer.Close()

	ms := s.(*memorySeriesStorage) // Going to test the internal purgeSeries method.

	s.AppendSamples(samples)

	fp := clientmodel.Metric{}.Fingerprint()

	// Purge ~half of the chunks.
	ms.purgeSeries(fp, 1000)
	it := s.NewIterator(fp)
	actual := it.GetBoundaryValues(metric.Interval{
		OldestInclusive: 0,
		NewestInclusive: 10000,
	})
	if len(actual) != 2 {
		t.Fatal("expected two results after purging half of series")
	}
	if actual[0].Timestamp < 800 || actual[0].Timestamp > 1000 {
		t.Errorf("1st timestamp out of expected range: %v", actual[0].Timestamp)
	}
	want := clientmodel.Timestamp(1998)
	if actual[1].Timestamp != want {
		t.Errorf("2nd timestamp: want %v, got %v", want, actual[1].Timestamp)
	}

	// Purge everything.
	ms.purgeSeries(fp, 10000)
	it = s.NewIterator(fp)
	actual = it.GetBoundaryValues(metric.Interval{
		OldestInclusive: 0,
		NewestInclusive: 10000,
	})
	if len(actual) != 0 {
		t.Fatal("expected zero results after purging the whole series")
	}

	// Recreate series.
	s.AppendSamples(samples)

	series, ok := ms.fpToSeries.get(fp)
	if !ok {
		t.Fatal("could not find series")
	}

	// Evict everything except head chunk.
	allEvicted, persistHeadChunk := series.evictOlderThan(1998)
	// Head chunk not yet old enough, should get false, false:
	if allEvicted {
		t.Error("allEvicted with head chunk not yet old enough")
	}
	if persistHeadChunk {
		t.Error("persistHeadChunk although head chunk is not old enough")
	}
	// Evict everything.
	allEvicted, persistHeadChunk = series.evictOlderThan(10000)
	// Since the head chunk is not yet persisted, we should get false, true:
	if allEvicted {
		t.Error("allEvicted with head chuk not yet persisted")
	}
	if !persistHeadChunk {
		t.Error("not persistHeadChunk although head chunk is old enough")
	}
	// Persist head chunk as requested.
	ms.persistQueue <- persistRequest{fp, series.head()}
	time.Sleep(time.Second) // Give time for persisting to happen.
	allEvicted, persistHeadChunk = series.evictOlderThan(10000)
	// Now we should really see everything gone.
	if !allEvicted {
		t.Error("not allEvicted")
	}
	if persistHeadChunk {
		t.Error("persistHeadChunk although already persisted")
	}

	// Now archive as it would usually be done in the evictTicker loop.
	ms.fpToSeries.del(fp)
	if err := ms.persistence.archiveMetric(
		fp, series.metric, series.firstTime(), series.lastTime(),
	); err != nil {
		t.Fatal(err)
	}

	archived, _, _, err := ms.persistence.hasArchivedMetric(fp)
	if err != nil {
		t.Fatal(err)
	}
	if !archived {
		t.Fatal("not archived")
	}

	// Purge ~half of the chunks of an archived series.
	ms.purgeSeries(fp, 1000)
	archived, _, _, err = ms.persistence.hasArchivedMetric(fp)
	if err != nil {
		t.Fatal(err)
	}
	if !archived {
		t.Fatal("archived series dropped although only half of the chunks purged")
	}

	// Purge everything.
	ms.purgeSeries(fp, 10000)
	archived, _, _, err = ms.persistence.hasArchivedMetric(fp)
	if err != nil {
		t.Fatal(err)
	}
	if archived {
		t.Fatal("archived series not dropped")
	}
}

func BenchmarkAppend(b *testing.B) {
	samples := make(clientmodel.Samples, b.N)
	for i := range samples {
		samples[i] = &clientmodel.Sample{
			Metric: clientmodel.Metric{
				clientmodel.MetricNameLabel: clientmodel.LabelValue(fmt.Sprintf("test_metric_%d", i%10)),
				"label1":                    clientmodel.LabelValue(fmt.Sprintf("test_metric_%d", i%10)),
				"label2":                    clientmodel.LabelValue(fmt.Sprintf("test_metric_%d", i%10)),
			},
			Timestamp: clientmodel.Timestamp(i),
			Value:     clientmodel.SampleValue(i),
		}
	}
	b.ResetTimer()
	s, closer := NewTestStorage(b)
	defer closer.Close()

	s.AppendSamples(samples)
}

func TestFuzz(t *testing.T) {
	r := rand.New(rand.NewSource(42))

	check := func() bool {
		s, c := NewTestStorage(t)
		defer c.Close()

		samples := createRandomSamples(r)
		s.AppendSamples(samples)

		return verifyStorage(t, s, samples, r)
	}

	if err := quick.Check(check, nil); err != nil {
		t.Fatal(err)
	}
}

func createRandomSamples(r *rand.Rand) clientmodel.Samples {
	type valueCreator func() clientmodel.SampleValue
	type deltaApplier func(clientmodel.SampleValue) clientmodel.SampleValue

	var (
		maxMetrics         = 5
		maxCycles          = 500
		maxStreakLength    = 500
		timestamp          = time.Now().Unix()
		maxTimeDelta       = 1000
		maxTimeDeltaFactor = 10
		generators         = []struct {
			createValue valueCreator
			applyDelta  []deltaApplier
		}{
			{ // "Boolean".
				createValue: func() clientmodel.SampleValue {
					return clientmodel.SampleValue(r.Intn(2))
				},
				applyDelta: []deltaApplier{
					func(_ clientmodel.SampleValue) clientmodel.SampleValue {
						return clientmodel.SampleValue(r.Intn(2))
					},
				},
			},
			{ // Integer with int deltas of various byte length.
				createValue: func() clientmodel.SampleValue {
					return clientmodel.SampleValue(r.Int63() - 1<<62)
				},
				applyDelta: []deltaApplier{
					func(v clientmodel.SampleValue) clientmodel.SampleValue {
						return clientmodel.SampleValue(r.Intn(1<<8) - 1<<7 + int(v))
					},
					func(v clientmodel.SampleValue) clientmodel.SampleValue {
						return clientmodel.SampleValue(r.Intn(1<<16) - 1<<15 + int(v))
					},
					func(v clientmodel.SampleValue) clientmodel.SampleValue {
						return clientmodel.SampleValue(r.Intn(1<<32) - 1<<31 + int(v))
					},
				},
			},
			{ // Float with float32 and float64 deltas.
				createValue: func() clientmodel.SampleValue {
					return clientmodel.SampleValue(r.NormFloat64())
				},
				applyDelta: []deltaApplier{
					func(v clientmodel.SampleValue) clientmodel.SampleValue {
						return v + clientmodel.SampleValue(float32(r.NormFloat64()))
					},
					func(v clientmodel.SampleValue) clientmodel.SampleValue {
						return v + clientmodel.SampleValue(r.NormFloat64())
					},
				},
			},
		}
	)

	result := clientmodel.Samples{}

	metrics := []clientmodel.Metric{}
	for n := r.Intn(maxMetrics); n >= 0; n-- {
		metrics = append(metrics, clientmodel.Metric{
			clientmodel.LabelName(fmt.Sprintf("labelname_%d", n+1)): clientmodel.LabelValue(fmt.Sprintf("labelvalue_%d", n+1)),
		})
	}

	for n := r.Intn(maxCycles); n >= 0; n-- {
		// Pick a metric for this cycle.
		metric := metrics[r.Intn(len(metrics))]
		timeDelta := r.Intn(maxTimeDelta) + 1
		generator := generators[r.Intn(len(generators))]
		createValue := generator.createValue
		applyDelta := generator.applyDelta[r.Intn(len(generator.applyDelta))]
		incTimestamp := func() { timestamp += int64(timeDelta * (r.Intn(maxTimeDeltaFactor) + 1)) }
		switch r.Intn(4) {
		case 0: // A single sample.
			result = append(result, &clientmodel.Sample{
				Metric:    metric,
				Value:     createValue(),
				Timestamp: clientmodel.TimestampFromUnix(timestamp),
			})
			incTimestamp()
		case 1: // A streak of random sample values.
			for n := r.Intn(maxStreakLength); n >= 0; n-- {
				result = append(result, &clientmodel.Sample{
					Metric:    metric,
					Value:     createValue(),
					Timestamp: clientmodel.TimestampFromUnix(timestamp),
				})
				incTimestamp()
			}
		case 2: // A streak of sample values with incremental changes.
			value := createValue()
			for n := r.Intn(maxStreakLength); n >= 0; n-- {
				result = append(result, &clientmodel.Sample{
					Metric:    metric,
					Value:     value,
					Timestamp: clientmodel.TimestampFromUnix(timestamp),
				})
				incTimestamp()
				value = applyDelta(value)
			}
		case 3: // A streak of constant sample values.
			value := createValue()
			for n := r.Intn(maxStreakLength); n >= 0; n-- {
				result = append(result, &clientmodel.Sample{
					Metric:    metric,
					Value:     value,
					Timestamp: clientmodel.TimestampFromUnix(timestamp),
				})
				incTimestamp()
			}
		}
	}

	return result
}

func verifyStorage(t *testing.T, s Storage, samples clientmodel.Samples, r *rand.Rand) bool {
	iters := map[clientmodel.Fingerprint]SeriesIterator{}
	result := true
	for _, i := range r.Perm(len(samples)) {
		sample := samples[i]
		fp := sample.Metric.Fingerprint()
		iter, ok := iters[fp]
		if !ok {
			iter = s.NewIterator(fp)
			iters[fp] = iter
		}
		found := iter.GetValueAtTime(sample.Timestamp)
		if len(found) != 1 {
			t.Errorf("Expected exactly one value, found %d.", len(found))
			return false
		}
		want := float64(sample.Value)
		got := float64(found[0].Value)
		if want != got {
			t.Errorf("Value mismatch, want %f, got %f.", want, got)
			result = false
		}
	}
	return result
}
