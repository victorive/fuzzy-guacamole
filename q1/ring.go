package q1

import (
	"sort"
	"strconv"
	"sync"
)

type ring struct {
	mu       sync.RWMutex
	replicas int
	backends map[string]int // name -> weight
	points   []ringPoint
}

type ringPoint struct {
	hash uint64
	name string
}

func newRing(replicas int) *ring {
	if replicas <= 0 {
		replicas = 100
	}
	return &ring{
		replicas: replicas,
		backends: make(map[string]int),
	}
}

func (r *ring) Add(name string, weight int) {
	if weight <= 0 {
		weight = 1
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.backends[name] = weight
	r.rebuildLocked()
}

func (r *ring) AddAll(backends map[string]int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for name, weight := range backends {
		if weight <= 0 {
			weight = 1
		}
		r.backends[name] = weight
	}
	r.rebuildLocked()
}

func (r *ring) Remove(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.backends, name)
	r.rebuildLocked()
}

func (r *ring) rebuildLocked() {
	r.points = r.points[:0]
	for name, weight := range r.backends {
		for i := 0; i < r.replicas*weight; i++ {
			r.points = append(r.points, ringPoint{
				hash: hashString(name + "#" + strconv.Itoa(i)),
				name: name,
			})
		}
	}
	sort.Slice(r.points, func(i, j int) bool {
		if r.points[i].hash == r.points[j].hash {
			return r.points[i].name < r.points[j].name
		}
		return r.points[i].hash < r.points[j].hash
	})
}

func (r *ring) Candidates(key string, n int) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.points) == 0 {
		return nil
	}
	if n <= 0 || n > len(r.points) {
		n = len(r.points)
	}

	h := hashString(key)
	start := sort.Search(len(r.points), func(i int) bool {
		return r.points[i].hash >= h
	})
	if start == len(r.points) {
		start = 0
	}

	out := make([]string, 0, n)
	seen := make(map[string]struct{}, n)
	for i := 0; i < len(r.points) && len(out) < n; i++ {
		p := r.points[(start+i)%len(r.points)]
		if _, ok := seen[p.name]; ok {
			continue
		}
		seen[p.name] = struct{}{}
		out = append(out, p.name)
	}
	return out
}

func (r *ring) Backends() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.backends))
	for name := range r.backends {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// hashString is FNV-1a followed by a 64-bit avalanche mix. FNV alone is stable
// but has poor high-bit distribution for short, sequential strings such as
// virtual-node names; the finalizer avoids heavily skewed rings. It is stable
// across processes, unlike maphash, which is seeded per process and would give
// different rings to different clients.
func hashString(s string) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	var h uint64 = offset64
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime64
	}
	// MurmurHash3 finalizer.
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	return h
}
