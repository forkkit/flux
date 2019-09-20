package sql

import (
	"context"
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/codes"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/internal/errors"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/flux/plan"
	"github.com/influxdata/flux/semantic"
	_ "github.com/lib/pq"
)

const FromSQLKind = "fromSQL"

// For SQL DATETIME parsing
const layout = "2006-01-02 15:04:05.999999999"

type FromSQLOpSpec struct {
	DriverName     string `json:"driverName,omitempty"`
	DataSourceName string `json:"dataSourceName,omitempty"`
	Query          string `json:"query,omitempty"`
}

func init() {
	fromSQLSignature := semantic.FunctionPolySignature{
		Parameters: map[string]semantic.PolyType{
			"driverName":     semantic.String,
			"dataSourceName": semantic.String,
			"query":          semantic.String,
		},
		Required: semantic.LabelSet{"driverName", "dataSourceName", "query"},
		Return:   flux.TableObjectType,
	}
	flux.RegisterPackageValue("sql", "from", flux.FunctionValue(FromSQLKind, createFromSQLOpSpec, fromSQLSignature))
	flux.RegisterOpSpec(FromSQLKind, newFromSQLOp)
	plan.RegisterProcedureSpec(FromSQLKind, newFromSQLProcedure, FromSQLKind)
	execute.RegisterSource(FromSQLKind, createFromSQLSource)
}

func createFromSQLOpSpec(args flux.Arguments, administration *flux.Administration) (flux.OperationSpec, error) {
	spec := new(FromSQLOpSpec)

	if driverName, err := args.GetRequiredString("driverName"); err != nil {
		return nil, err
	} else {
		spec.DriverName = driverName
	}

	if dataSourceName, err := args.GetRequiredString("dataSourceName"); err != nil {
		return nil, err
	} else {
		spec.DataSourceName = dataSourceName
	}

	if query, err := args.GetRequiredString("query"); err != nil {
		return nil, err
	} else {
		spec.Query = query
	}

	return spec, nil
}

func newFromSQLOp() flux.OperationSpec {
	return new(FromSQLOpSpec)
}

func (s *FromSQLOpSpec) Kind() flux.OperationKind {
	return FromSQLKind
}

type FromSQLProcedureSpec struct {
	plan.DefaultCost
	DriverName     string
	DataSourceName string
	Query          string
}

func newFromSQLProcedure(qs flux.OperationSpec, pa plan.Administration) (plan.ProcedureSpec, error) {
	spec, ok := qs.(*FromSQLOpSpec)
	if !ok {
		return nil, errors.Newf(codes.Internal, "invalid spec type %T", qs)
	}

	return &FromSQLProcedureSpec{
		DriverName:     spec.DriverName,
		DataSourceName: spec.DataSourceName,
		Query:          spec.Query,
	}, nil
}

func (s *FromSQLProcedureSpec) Kind() plan.ProcedureKind {
	return FromSQLKind
}

func (s *FromSQLProcedureSpec) Copy() plan.ProcedureSpec {
	ns := new(FromSQLProcedureSpec)
	ns.DriverName = s.DriverName
	ns.DataSourceName = s.DataSourceName
	ns.Query = s.Query
	return ns
}

func createFromSQLSource(prSpec plan.ProcedureSpec, dsid execute.DatasetID, a execute.Administration) (execute.Source, error) {
	spec, ok := prSpec.(*FromSQLProcedureSpec)
	if !ok {
		return nil, errors.Newf(codes.Internal, "invalid spec type %T", prSpec)
	}

	// Retrieve the row reader implementation for the driver.
	var newRowReader func(rows *sql.Rows) (execute.RowReader, error)
	switch spec.DriverName {
	case "mysql":
		newRowReader = NewMySQLRowReader
	case "postgres", "sqlmock":
		newRowReader = NewPostgresRowReader
	default:
		return nil, errors.Newf(codes.Invalid, "sql driver %s not supported", spec.DriverName)
	}

	readFn := func(ctx context.Context, rows *sql.Rows) (flux.Table, error) {
		reader, err := newRowReader(rows)
		if err != nil {
			_ = rows.Close()
			return nil, err
		}
		return read(ctx, reader, a.Allocator())
	}
	iterator := &sqlIterator{spec: spec, id: dsid, read: readFn}
	return execute.CreateSourceFromIterator(iterator, dsid)
}

var _ execute.SourceIterator = (*sqlIterator)(nil)

type sqlIterator struct {
	spec  *FromSQLProcedureSpec
	id    execute.DatasetID
	read  func(ctx context.Context, rows *sql.Rows) (flux.Table, error)
	table flux.Table
	err   error
	done  bool
}

func (c *sqlIterator) connect(ctx context.Context) (*sql.DB, error) {
	db, err := sql.Open(c.spec.DriverName, c.spec.DataSourceName)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func (c *sqlIterator) Next(ctx context.Context) bool {
	if c.done {
		return false
	}
	defer func() { c.done = true }()

	// Connect to the database so we can execute the query.
	db, err := c.connect(ctx)
	if err != nil {
		c.err = err
		return false
	}
	defer func() { _ = db.Close() }()

	rows, err := db.QueryContext(ctx, c.spec.Query)
	if err != nil {
		c.err = err
		return false
	}
	defer func() { _ = rows.Close() }()

	table, err := c.read(ctx, rows)
	if err != nil {
		c.err = err
		return false
	}
	c.table = table
	return true
}

func (c *sqlIterator) Table() flux.Table {
	return c.table
}

func (c *sqlIterator) Release() {
	// This iterator does not maintain any internal connections.
}

func (c *sqlIterator) Err() error {
	return c.err
}

// read will use the RowReader to construct a flux.Table.
func read(ctx context.Context, reader execute.RowReader, alloc *memory.Allocator) (flux.Table, error) {
	// Ensure that the reader is always freed so the underlying
	// cursor can be returned.
	defer func() { _ = reader.Close() }()

	groupKey := execute.NewGroupKey(nil, nil)
	builder := execute.NewColListTableBuilder(groupKey, alloc)
	for i, dataType := range reader.ColumnTypes() {
		if _, err := builder.AddCol(flux.ColMeta{Label: reader.ColumnNames()[i], Type: dataType}); err != nil {
			return nil, err
		}
	}

	for reader.Next() {
		row, err := reader.GetNextRow()
		if err != nil {
			return nil, err
		}

		for i, col := range row {
			if err := builder.AppendValue(i, col); err != nil {
				return nil, err
			}
		}
	}

	// An error may have been encountered while reading.
	// This will get reported when we go to close the reader.
	if err := reader.Close(); err != nil {
		return nil, err
	}
	return builder.Table()
}
