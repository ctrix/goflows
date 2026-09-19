package goflows

// countSubscriptions returns the number of live subscriptions across every
// bus and event type. Test helper: it reads the same snapshot dispatchers use.
func (c *Engine) countSubscriptions() int64 {
	var n int64
	for _, subs := range *c.subscriptions.Load() {
		n += int64(len(subs))
	}
	return n
}
