package universe

import (
	"fmt"
	"math"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/plan"
	"github.com/influxdata/flux/semantic"
)

const HoltWintersKind = "holtWinters"

func init() {
	holtWintersSignature := flux.FunctionSignature(
		map[string]semantic.PolyType{
			"interval": semantic.Duration,
			"points":   semantic.Int,
			"offset":   semantic.Duration,
			"season":   semantic.Int,
		},
		[]string{"interval", "points", "offset", "season"},
	)

	flux.RegisterPackageValue("universe", HoltWintersKind, flux.FunctionValue(
		HoltWintersKind,
		createHoltWintersOpSpec,
		holtWintersSignature,
	))
	flux.RegisterOpSpec(HoltWintersKind, newHoltWintersOpSpec)
	plan.RegisterProcedureSpec(HoltWintersKind, newHoltWintersProcedureSpec, HoltWintersKind)
	execute.RegisterTransformation(HoltWintersKind, createHWTransformation)
}

func createHoltWintersOpSpec(args flux.Arguments, a *flux.Administration) (flux.OperationSpec, error) {
	if err := a.AddParentFromArgs(args); err != nil {
		return nil, err
	}

	spec := new(HoltWintersOpSpec)

	if interval, err := args.GetRequiredDuration("interval"); err != nil {
		return nil, err
	} else {
		spec.Interval = interval
	}

	if offset, err := args.GetRequiredDuration("offset"); err != nil {
		return nil, err
	} else {
		spec.Offset = offset
	}

	if points, err := args.GetRequiredInt("points"); err != nil {
		return nil, err
	} else {
		spec.Points = points
	}

	if season, err := args.GetRequiredInt("season"); err != nil {
		return nil, err
	} else {
		spec.Season = season
	}

	return spec, nil
}

type HoltWintersOpSpec struct {
	Interval flux.Duration
	Offset   flux.Duration
	Points   int64
	Season   int64
}

func newHoltWintersOpSpec() flux.OperationSpec {
	return new(HoltWintersOpSpec)
}

func (s *HoltWintersOpSpec) Kind() flux.OperationKind {
	return HoltWintersKind
}

const (
	// Arbitrary weight for initializing some intial guesses.
	// This should be in the  range [0,1]
	hwWeight = 0.5
	// Epsilon value for the minimization process
	hwDefaultEpsilon = 1.0e-4
	// Define a grid of initial guesses for the parameters: alpha, beta, gamma, and phi.
	// Keep in mind that this grid is N^4 so we should keep N small
	// The starting lower guess
	hwGuessLower = 0.3
	//  The upper bound on the grid
	hwGuessUpper = 1.0
	// The step between guesses
	hwGuessStep = 0.4
)

type HWProcedureSpec struct {
	plan.DefaultCost
	op *HoltWintersOpSpec

	// Season period
	m        int
	seasonal bool

	// Horizon
	h int

	// Interval between points
	interval int64
	// interval / 2 -- used to perform rounding
	halfInterval int64

	// Whether to include all data or only future values
	includeFitData bool

	// NelderMead optimizer
	optim *Optimizer
	// Small difference bound for the optimizer
	epsilon float64

	y      []float64
	points []FloatPoint
}

func (r *HWProcedureSpec) roundTime(t int64) int64 {
	// Overflow safe round function
	remainder := t % r.interval
	if remainder > r.halfInterval {
		// Round up
		return (t/r.interval + 1) * r.interval
	}
	// Round down
	return (t / r.interval) * r.interval
}

// Forecast the data h points into the future.
func (r *HWProcedureSpec) forecast(h int, params []float64) []float64 {
	// Constrain parameters
	constrain(params)

	yT := r.y[0]

	phi := params[3]
	phiH := phi

	lT := params[4]
	bT := params[5]

	// seasonals is a ring buffer of past sT values
	var seasonals []float64
	var m, so int
	if r.seasonal {
		seasonals = params[6:]
		m = len(params[6:])
		if m == 1 {
			seasonals[0] = 1
		}
		// Season index offset
		so = m - 1
	}

	forecasted := make([]float64, len(r.y)+h)
	forecasted[0] = yT
	l := len(r.y)
	var hm int
	stm, stmh := 1.0, 1.0
	for t := 1; t < l+h; t++ {
		if r.seasonal {
			hm = t % m
			stm = seasonals[(t-m+so)%m]
			stmh = seasonals[(t-m+hm+so)%m]
		}
		var sT float64
		yT, lT, bT, sT = r.next(
			params[0], // alpha
			params[1], // beta
			params[2], // gamma
			phi,
			phiH,
			yT,
			lT,
			bT,
			stm,
			stmh,
		)
		phiH += math.Pow(phi, float64(t))

		if r.seasonal {
			seasonals[(t+so)%m] = sT
			so++
		}

		forecasted[t] = yT
	}
	return forecasted
}

// Compute sum squared error for the given parameters.
func (r *HWProcedureSpec) sse(params []float64) float64 {
	sse := 0.0
	forecasted := r.forecast(0, params)
	for i := range forecasted {
		// Skip missing values since we cannot use them to compute an error.
		if !math.IsNaN(r.y[i]) {
			// Compute error
			if math.IsNaN(forecasted[i]) {
				// Penalize forecasted NaNs
				return math.Inf(1)
			}
			diff := forecasted[i] - r.y[i]
			sse += diff * diff
		}
	}
	return sse
}

func newHoltWintersProcedureSpec(qs flux.OperationSpec, pa plan.Administration) (plan.ProcedureSpec, error) {
	spec, ok := qs.(*HoltWintersOpSpec)
	if !ok {
		return nil, fmt.Errorf("invalid spec type %T", qs)
	}
	return &HWProcedureSpec{
		op: spec,
	}, nil
}

func (s *HWProcedureSpec) Kind() plan.ProcedureKind {
	return FilterKind
}
func (s *HWProcedureSpec) Copy() plan.ProcedureSpec {
	ns := new(HWProcedureSpec)
	ns.op.Interval = s.op.Interval
	ns.op.Offset = s.op.Offset
	ns.op.Points = s.op.Points
	ns.op.Season = s.op.Season
	return ns
}

func createHWTransformation(id execute.DatasetID, mode execute.AccumulationMode, spec plan.ProcedureSpec, a execute.Administration) (execute.Transformation, execute.Dataset, error) {
	s, ok := spec.(*HWProcedureSpec)
	if !ok {
		return nil, nil, fmt.Errorf("invalid spec type %T", spec)
	}
	cache := execute.NewTableBuilderCache(a.Allocator())
	d := execute.NewDataset(id, mode, cache)
	t, err := NewHoltWintersTransformation(d, cache, s)
	if err != nil {
		return nil, nil, err
	}
	return t, d, nil
}

type hwTransformation struct {
	d     execute.Dataset
	cache execute.TableBuilderCache
}

func NewHoltWintersTransformation(d execute.Dataset, cache execute.TableBuilderCache, spec *HWProcedureSpec) (*hwTransformation, error) {
	return &hwTransformation{
		d:     d,
		cache: cache,
	}, nil
}

func (t *hwTransformation) Process(id execute.DatasetID, tbl flux.Table) error {
	return nil
}

func (t *hwTransformation) RetractTable(id execute.DatasetID, key flux.GroupKey) error {
	return t.d.RetractTable(key)
}
func (t *hwTransformation) UpdateWatermark(id execute.DatasetID, mark execute.Time) error {
	return t.d.UpdateWatermark(mark)
}
func (t *hwTransformation) UpdateProcessingTime(id execute.DatasetID, pt execute.Time) error {
	return t.d.UpdateProcessingTime(pt)
}
func (t *hwTransformation) Finish(id execute.DatasetID, err error) {
	t.d.Finish(err)
}

// Using the recursive relations compute the next values
func next(alpha, beta, gamma, phi, phiH, yT, lTp, bTp, sTm, sTmh float64) (yTh, lT, bT, sT float64) {
	lT = alpha*(yT/sTm) + (1-alpha)*(lTp+phi*bTp)
	bT = beta*(lT-lTp) + (1-beta)*phi*bTp
	sT = gamma*(yT/(lTp+phi*bTp)) + (1-gamma)*sTm
	yTh = (lT + phiH*bT) * sTmh
	return
}

// Constrain alpha, beta, gamma, phi in the range [0, 1]
func constrain(x []float64) {
	// alpha
	if x[0] > 1 {
		x[0] = 1
	}
	if x[0] < 0 {
		x[0] = 0
	}
	// beta
	if x[1] > 1 {
		x[1] = 1
	}
	if x[1] < 0 {
		x[1] = 0
	}
	// gamma
	if x[2] > 1 {
		x[2] = 1
	}
	if x[2] < 0 {
		x[2] = 0
	}
	// phi
	if x[3] > 1 {
		x[3] = 1
	}
	if x[3] < 0 {
		x[3] = 0
	}
}

const (
	defaultMaxIterations = 1000
	// reflection coefficient
	defaultAlpha = 1.0
	// contraction coefficient
	defaultBeta = 0.5
	// expansion coefficient
	defaultGamma = 2.0
)

// Optimizer represents the parameters to the Nelder-Mead simplex method.
type Optimizer struct {
	// Maximum number of iterations.
	MaxIterations int
	// Reflection coefficient.
	Alpha,
	// Contraction coefficient.
	Beta,
	// Expansion coefficient.
	Gamma float64
}

// New returns a new instance of Optimizer with all values set to the defaults.
func New() *Optimizer {
	return &Optimizer{
		MaxIterations: defaultMaxIterations,
		Alpha:         defaultAlpha,
		Beta:          defaultBeta,
		Gamma:         defaultGamma,
	}
}

// Optimize applies the Nelder-Mead simplex method with the Optimizer's settings.
// Based on work by Michael F. Hutt: http://www.mikehutt.com/neldermead.html
func (o *Optimizer) Optimize(
	objfunc func([]float64) float64,
	start []float64,
	epsilon,
	scale float64,
) (float64, []float64) {
	n := len(start)

	//holds vertices of simplex
	v := make([][]float64, n+1)
	for i := range v {
		v[i] = make([]float64, n)
	}

	//value of function at each vertex
	f := make([]float64, n+1)

	//reflection - coordinates
	vr := make([]float64, n)

	//expansion - coordinates
	ve := make([]float64, n)

	//contraction - coordinates
	vc := make([]float64, n)

	//centroid - coordinates
	vm := make([]float64, n)

	// create the initial simplex
	// assume one of the vertices is 0,0

	pn := scale * (math.Sqrt(float64(n+1)) - 1 + float64(n)) / (float64(n) * math.Sqrt(2))
	qn := scale * (math.Sqrt(float64(n+1)) - 1) / (float64(n) * math.Sqrt(2))

	for i := 0; i < n; i++ {
		v[0][i] = start[i]
	}

	for i := 1; i <= n; i++ {
		for j := 0; j < n; j++ {
			if i-1 == j {
				v[i][j] = pn + start[j]
			} else {
				v[i][j] = qn + start[j]
			}
		}
	}

	// find the initial function values
	for j := 0; j <= n; j++ {
		f[j] = objfunc(v[j])
	}

	// begin the main loop of the minimization
	for itr := 1; itr <= o.MaxIterations; itr++ {

		// find the indexes of the largest and smallest values
		vg := 0
		vs := 0
		for i := 0; i <= n; i++ {
			if f[i] > f[vg] {
				vg = i
			}
			if f[i] < f[vs] {
				vs = i
			}
		}
		// find the index of the second largest value
		vh := vs
		for i := 0; i <= n; i++ {
			if f[i] > f[vh] && f[i] < f[vg] {
				vh = i
			}
		}

		// calculate the centroid
		for i := 0; i <= n-1; i++ {
			cent := 0.0
			for m := 0; m <= n; m++ {
				if m != vg {
					cent += v[m][i]
				}
			}
			vm[i] = cent / float64(n)
		}

		// reflect vg to new vertex vr
		for i := 0; i <= n-1; i++ {
			vr[i] = vm[i] + o.Alpha*(vm[i]-v[vg][i])
		}

		// value of function at reflection point
		fr := objfunc(vr)

		if fr < f[vh] && fr >= f[vs] {
			for i := 0; i <= n-1; i++ {
				v[vg][i] = vr[i]
			}
			f[vg] = fr
		}

		// investigate a step further in this direction
		if fr < f[vs] {
			for i := 0; i <= n-1; i++ {
				ve[i] = vm[i] + o.Gamma*(vr[i]-vm[i])
			}

			// value of function at expansion point
			fe := objfunc(ve)

			// by making fe < fr as opposed to fe < f[vs],
			// Rosenbrocks function takes 63 iterations as opposed
			// to 64 when using double variables.

			if fe < fr {
				for i := 0; i <= n-1; i++ {
					v[vg][i] = ve[i]
				}
				f[vg] = fe
			} else {
				for i := 0; i <= n-1; i++ {
					v[vg][i] = vr[i]
				}
				f[vg] = fr
			}
		}

		// check to see if a contraction is necessary
		if fr >= f[vh] {
			if fr < f[vg] && fr >= f[vh] {
				// perform outside contraction
				for i := 0; i <= n-1; i++ {
					vc[i] = vm[i] + o.Beta*(vr[i]-vm[i])
				}
			} else {
				// perform inside contraction
				for i := 0; i <= n-1; i++ {
					vc[i] = vm[i] - o.Beta*(vm[i]-v[vg][i])
				}
			}

			// value of function at contraction point
			fc := objfunc(vc)

			if fc < f[vg] {
				for i := 0; i <= n-1; i++ {
					v[vg][i] = vc[i]
				}
				f[vg] = fc
			} else {
				// at this point the contraction is not successful,
				// we must halve the distance from vs to all the
				// vertices of the simplex and then continue.

				for row := 0; row <= n; row++ {
					if row != vs {
						for i := 0; i <= n-1; i++ {
							v[row][i] = v[vs][i] + (v[row][i]-v[vs][i])/2.0
						}
					}
				}
				f[vg] = objfunc(v[vg])
				f[vh] = objfunc(v[vh])
			}
		}

		// test for convergence
		fsum := 0.0
		for i := 0; i <= n; i++ {
			fsum += f[i]
		}
		favg := fsum / float64(n+1)
		s := 0.0
		for i := 0; i <= n; i++ {
			s += math.Pow((f[i]-favg), 2.0) / float64(n)
		}
		s = math.Sqrt(s)
		if s < epsilon {
			break
		}
	}

	// find the index of the smallest value
	vs := 0
	for i := 0; i <= n; i++ {
		if f[i] < f[vs] {
			vs = i
		}
	}

	parameters := make([]float64, n)
	for i := 0; i < n; i++ {
		parameters[i] = v[vs][i]
	}

	min := objfunc(v[vs])

	return min, parameters
}
