package backpressure

import (
	"math"
	"time"
)

// 'x' will be substituted by the debt value in actual usage
// linearDecay is a value in [0, max] that decays linearly towards zero over time
type linearDecay struct {
	// x is the value of linearDecay as of last
	x float64
	// last is the last time x was computed
	last time.Time
	// decayPerSec is subtracted from x every second
	decayPerSec float64
	// max is the maximum value of x
	max float64
}

// adds a value to the debt (x)
func (d *linearDecay) add(now time.Time, x float64) {
	d.get(now)
	d.x += x
	if d.x > d.max {
		d.x = d.max
	}
	if d.x < 0 {
		d.x = 0
	}
}

// accelerate or deaccelerate decay speed
// used to control low priority requests' passage
// based on high priority requests' current situation
func (d *linearDecay) setDecayPerSec(now time.Time, decayPerSec float64) {
	d.get(now)
	d.decayPerSec = decayPerSec
}

func (d *linearDecay) setMax(now time.Time, max float64) {
	d.get(now)
	d.max = max
	if d.x > d.max {
		d.x = d.max
	}
}

// floor will "cap" the current value equal to or below the floor value
func (d *linearDecay) floor(now time.Time, floor float64) {
	d.get(now)
	if d.x < floor {
		d.x = floor
	}
}

// we don't calculate the decay after every sec (we can but mehh unoptimised)
// we keep track a last recorded timestamp and the actual value at that time
// before any other operation is actually carried out (all the above ops)
// the get() func calculates the current value and updates both timestamp and the value
func (d *linearDecay) get(now time.Time) float64 {
	d.x = math.Max(0, d.x-math.Max(0, now.Sub(d.last).Seconds())*d.decayPerSec)
	d.last = now
	return d.x
}
