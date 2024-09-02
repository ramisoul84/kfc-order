package domain

import "testing"

func TestStatusTransitions(t *testing.T) {
	cases := []struct {
		from, to Status
		ok       bool
	}{
		{StatusAccepted, StatusPreparing, true},
		{StatusAccepted, StatusReady, true},
		{StatusAccepted, StatusCancelled, true},
		{StatusPreparing, StatusReady, true},
		{StatusPreparing, StatusAccepted, true},
		{StatusReady, StatusCompleted, true},
		{StatusReady, StatusPreparing, true}, // recall
		{StatusReady, StatusCancelled, false},
		{StatusAccepted, StatusCompleted, false},
		{StatusCompleted, StatusReady, false},
		{StatusCancelled, StatusAccepted, false},
	}
	for _, c := range cases {
		if got := c.from.CanTransitionTo(c.to); got != c.ok {
			t.Errorf("%s → %s: got %v, want %v", c.from, c.to, got, c.ok)
		}
	}
}

func TestToViewConvertsKopecks(t *testing.T) {
	o := &Order{Subtotal: 45_750, Total: 45_750, Lines: []OrderLine{{UnitPrice: 29_900, LineTotal: 29_900, Quantity: 1}}}
	v := o.ToView()
	if v.Total != 457.50 || v.Lines[0].UnitPrice != 299.00 {
		t.Fatalf("unexpected amounts: total=%v unit=%v", v.Total, v.Lines[0].UnitPrice)
	}
}
