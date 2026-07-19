package catalog

import "sync"

type keyedGate struct {
	mu    sync.Mutex
	gates map[string]*gateEntry
}

type gateEntry struct {
	mu    sync.Mutex
	users int
}

func (g *keyedGate) lock(key string) func() {
	g.mu.Lock()
	if g.gates == nil {
		g.gates = map[string]*gateEntry{}
	}
	entry := g.gates[key]
	if entry == nil {
		entry = &gateEntry{}
		g.gates[key] = entry
	}
	entry.users++
	g.mu.Unlock()

	entry.mu.Lock()
	return func() {
		entry.mu.Unlock()
		g.mu.Lock()
		entry.users--
		if entry.users == 0 {
			delete(g.gates, key)
		}
		g.mu.Unlock()
	}
}
