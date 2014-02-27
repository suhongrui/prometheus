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
	"fmt"

	clientmodel "github.com/prometheus/client_golang/model"
)

// scanJob is an operation with a fingerprint of what to operate on.
type scanJob struct {
	fingerprint clientmodel.Fingerprint
	operation   op
}

func (s scanJob) String() string {
	return fmt.Sprintf("Scan Job { fingerprint=%s with operation [%s] ", s.fingerprint, s.operation)
}

// scanJobs is a heap of scanJobs, primary sorting key is the fingerprint.
type scanJobs []*scanJob

func (s scanJobs) Len() int {
	return len(s)
}

// Less compares the fingerprints. If they are equal, the operations are compared.
func (s scanJobs) Less(i, j int) bool {
	si := s[i]
	sj := s[j]
	if si.fingerprint.Equal(&sj.fingerprint) {
		return si.operation.StartsAt().Before(sj.operation.StartsAt())
	}
	return si.fingerprint.Less(&sj.fingerprint)
}

func (s scanJobs) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s *scanJobs) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*s = append(*s, x.(*scanJob))
}

func (s *scanJobs) Pop() interface{} {
	old := *s
	n := len(old)
	x := old[n-1]
	*s = old[0 : n-1]
	return x
}
