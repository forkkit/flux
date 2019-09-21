package promql

import (
	"context"
	"fmt"
	"sync"

	"github.com/influxdata/flux/values"

	"github.com/pkg/errors"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/interpreter"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/flux/plan"
	"github.com/influxdata/flux/semantic"
)

const JoinKind = "join"

func init() {
	joinSignature := semantic.FunctionPolySignature{
		Parameters: map[string]semantic.PolyType{
			"a":  flux.TableObjectType,
			"b":  flux.TableObjectType,
			"fn": semantic.Function,
		},
		Required: semantic.LabelSet{"a", "b", "fn"},
		Return:   flux.TableObjectType,
	}
	flux.RegisterPackageValue("universe", JoinKind, flux.FunctionValue(JoinKind, createJoinOpSpec, joinSignature))
	flux.RegisterOpSpec(JoinKind, newJoinOp)
	plan.RegisterProcedureSpec(JoinKind, newMergeJoinProcedure, JoinKind)
	execute.RegisterTransformation(JoinKind, createMergeJoinTransformation)
}

type JoinOpSpec struct {
	A  flux.OperationID             `json:"a"`
	B  flux.OperationID             `json:"b"`
	Fn interpreter.ResolvedFunction `json:"fn"`

	a, b *flux.TableObject
}

func (s *JoinOpSpec) IDer(ider flux.IDer) {
	s.A = ider.ID(s.a)
	s.B = ider.ID(s.b)
}

func createJoinOpSpec(args flux.Arguments, p *flux.Administration) (flux.OperationSpec, error) {
	a, err := args.GetRequiredObject("a")
	if err != nil {
		return nil, err
	}
	p.AddParent(a.(*flux.TableObject))

	b, err := args.GetRequiredObject("b")
	if err != nil {
		return nil, err
	}
	p.AddParent(b.(*flux.TableObject))

	f, err := args.GetRequiredFunction("fn")
	if err != nil {
		return nil, err
	}

	fn, err := interpreter.ResolveFunction(f)
	if err != nil {
		return nil, err
	}

	return &JoinOpSpec{
		Fn: fn,
	}, nil
}

func newJoinOp() flux.OperationSpec {
	return new(JoinOpSpec)
}

func (s *JoinOpSpec) Kind() flux.OperationKind {
	return JoinKind
}

type MergeJoinProcedureSpec struct {
	plan.DefaultCost

	Fn interpreter.ResolvedFunction `json:"fn"`
}

func newMergeJoinProcedure(qs flux.OperationSpec, pa plan.Administration) (plan.ProcedureSpec, error) {
	spec, ok := qs.(*JoinOpSpec)
	if !ok {
		return nil, fmt.Errorf("invalid spec type %T", qs)
	}
	return &MergeJoinProcedureSpec{
		Fn: spec.Fn,
	}, nil
}

func (s *MergeJoinProcedureSpec) Kind() plan.ProcedureKind {
	return JoinKind
}
func (s *MergeJoinProcedureSpec) Copy() plan.ProcedureSpec {
	return &MergeJoinProcedureSpec{
		Fn: s.Fn.Copy(),
	}
}

func createMergeJoinTransformation(id execute.DatasetID, mode execute.AccumulationMode, spec plan.ProcedureSpec, a execute.Administration) (execute.Transformation, execute.Dataset, error) {
	s, ok := spec.(*MergeJoinProcedureSpec)
	if !ok {
		return nil, nil, fmt.Errorf("invalid spec type %T", spec)
	}
	parents := a.Parents()

	cache := NewMergeJoinCache(a.Context(), a.Allocator(), parents[0], parents[1])
	d := execute.NewDataset(id, mode, cache)
	t := NewMergeJoinTransformation(d, cache)
	return t, d, nil
}

type mergeJoinTransformation struct {
	mu sync.Mutex

	d     execute.Dataset
	cache *mergeJoinCache
}

func NewMergeJoinTransformation(d execute.Dataset, cache *mergeJoinCache) *mergeJoinTransformation {
	return &mergeJoinTransformation{
		d:     d,
		cache: cache,
	}
}

func (t *mergeJoinTransformation) Process(id execute.DatasetID, tbl flux.Table) error {
	t.mu.Lock()
	t.cache.insert(id, tbl)
	t.mu.Unlock()
	return nil
}

func (t *mergeJoinTransformation) RetractTable(id execute.DatasetID, key flux.GroupKey) error {
	panic("not implemented")
}

func (t *mergeJoinTransformation) UpdateWatermark(id execute.DatasetID, mark execute.Time) error {
	return nil
}

func (t *mergeJoinTransformation) UpdateProcessingTime(id execute.DatasetID, pt execute.Time) error {
	return nil
}

func (t *mergeJoinTransformation) Finish(id execute.DatasetID, err error) {
}

func NewMergeJoinCache(ctx context.Context, alloc *memory.Allocator, a, b execute.DatasetID) *mergeJoinCache {
	return &mergeJoinCache{
		a:     a,
		b:     b,
		data:  execute.NewGroupLookup(),
		alloc: alloc,
		ctx:   ctx,
	}
}

type mergeJoinCache struct {
	a, b execute.DatasetID

	fn *execute.RowJoinFn

	data  *execute.GroupLookup
	alloc *memory.Allocator

	ctx context.Context
}

type cacheEntry struct {
	a, b flux.Table
}

func (c *mergeJoinCache) insert(id execute.DatasetID, tbl flux.Table) error {
	entry, ok := c.data.Lookup(tbl.Key())
	if !ok {
		if id == c.a {
			c.data.Set(tbl.Key(), &cacheEntry{a: tbl})
			return nil
		}
		if id == c.b {
			c.data.Set(tbl.Key(), &cacheEntry{b: tbl})
			return nil
		}
	}
	if id == c.a {
		entry.a = tbl
	}
	if id == c.b {
		entry.b = tbl
	}
	return nil
}

func (c *mergeJoinCache) Table(key flux.GroupKey) (flux.Table, error) {
	entry, ok := c.data.Lookup(key)
	if !ok {
		return nil, errors.New("table not found")
	}
	defer func() {
		c.data.Delete(key)
		entry.a.Done()
		entry.b.Done()
	}()
	return c.joinTables(entry.a, entry.b)
}

func (c *mergeJoinCache) ForEach(f func(flux.GroupKey)) {
	c.data.Range(func(key flux.GroupKey, value interface{}) {
		f(key)
	})
}

func (c *mergeJoinCache) ForEachWithContext(f func(flux.GroupKey, execute.Trigger, execute.TableContext)) {
	c.data.Range(func(key flux.GroupKey, value interface{}) {
		entry := value.(*cacheEntry)
		ctx := execute.TableContext{
			Key:   key,
			Count: entry.Size(),
		}
		f(key, entry.trigger, ctx)
	})
}

func (c *mergeJoinCache) DiscardTable(key flux.GroupKey) {
	v, ok := c.data.Lookup(key)
	if ok {
		entry := v.(*cacheEntry)
		entry.a.Done()
		entry.b.Done()
	}
}

func (c *mergeJoinCache) ExpireTable(key flux.GroupKey) {
	c.data.Delete(key)
}

func (c *mergeJoinCache) SetTriggerSpec(spec plan.TriggerSpec) {
}

func (c *mergeJoinCache) joinTables(a, b flux.Table) (flux.Table, error) {
	defer a.Done()
	defer b.Done()

	builder := execute.NewColListTableBuilder(a.Key(), c.alloc)

	ar := make([]flux.ColReader, 0)
	br := make([]flux.ColReader, 0)

	// Retain column readers for joining
	if err := a.Do(func(cr flux.ColReader) error {
		cr.Retain()
		ar = append(ar, cr)
		return nil
	}); err != nil {
		return nil, err
	}
	if err := b.Do(func(cr flux.ColReader) error {
		cr.Retain()
		br = append(br, cr)
		return nil
	}); err != nil {
		return nil, err
	}

	col := execute.ColIdx(execute.DefaultTimeColLabel, a.Cols())
	if col < 0 {
		return nil, errors.New("no _time column found")
	}

	aRows := records{
		row:     -1,
		col:     col,
		readers: ar,
	}

	col = execute.ColIdx(execute.DefaultTimeColLabel, b.Cols())
	if col < 0 {
		return nil, errors.New("no _time column found")
	}

	bRows := records{
		row:     -1,
		col:     col,
		readers: br,
	}

	var l, r int
	var t, s int64

	var aIdx, bIdx int
	var aTime, bTime int64
	var aReader, bReader flux.ColReader

NEXT:
	if aReader == nil || bReader == nil {
		goto DONE
	}

	aTime = aRows.timeValue()
	bTime = bRows.timeValue()

	if aTime < bTime {
		aIdx, aReader = aRows.next()
		goto NEXT
	}
	if t < s {
		bIdx, bReader = bRows.next()
		goto NEXT
	}
	if t == s {
		rowA := rowFromReader(aReader, aIdx)
		rowB := rowFromReader(bReader, bIdx)

		obj, err := c.fn.Eval(c.ctx, rowA, rowB)
		if err != nil {
			return nil, err
		}
		// check that group key is preserved in obj
		if err := execute.AppendRowToBuilder(builder, obj); err != nil {
			return nil, err
		}
		bIdx, bReader = bRows.next()
		goto NEXT
	}
DONE:
	return builder.Table()
}

func rowFromReader(cr flux.ColReader, row int) values.Object {
	for j, c := range cr.Cols() {
		// obj[c.Label] = execute.ValueForRow(cr, row, j)
	}
	return nil
}

type records struct {
	cur     int
	row     int
	col     int
	readers []flux.ColReader
}

func (r *records) next() (int, flux.ColReader) {
	r.row++
	reader := r.readers[r.cur]
	if r.row < reader.Len() {
		return r.row, reader
	}
	r.cur++
	if r.cur < len(r.readers) {
		r.row = 0
		return r.row, r.readers[r.cur]
	}
	return -1, nil
}

func (r *records) timeValue() int64 {
	return r.readers[r.cur].Times(r.col).Value(r.row)
}
