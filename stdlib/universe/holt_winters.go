package universe

import (
	"fmt"

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

type HWProcedureSpec struct {
	plan.DefaultCost
	op *HoltWintersOpSpec
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
