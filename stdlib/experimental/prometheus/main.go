package main

import (
	// External libraries
	"context"
	"fmt"
	"io"
	"math"
	"mime"
	"net/http"
	"time"

	// Flux packages
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/plan"
	"github.com/influxdata/flux/semantic"
	"github.com/influxdata/flux/values"

	// Prometheus packages
	"github.com/matttproud/golang_protobuf_extensions/pbutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

/*
We store data in rows, with the ability to query for metric data.
Prometheus stores data in metrics without specific timestamps

*/
const FromPrometheusKind = "fromPrometheus"

type FromPrometheusOpSpec struct {
}

func init() {
	fromPrometheusSignature := semantic.FunctionPolySignature{
		Parameters: map[string]semantic.PolyType{}, // user params
		Return:     flux.TableObjectType,
	}
	// telling the flux runtime about the objects that we're creating
	flux.RegisterPackageValue("prometheus", "from", flux.FunctionValue(FromPrometheusKind, createFromPrometheusOpSpec, fromPrometheusSignature))
	flux.RegisterOpSpec(FromPrometheusKind, newFromPrometheusOp)
	plan.RegisterProcedureSpec(FromPrometheusKind, newFromPrometheusProcedure, FromPrometheusKind)
	execute.RegisterSource(FromPrometheusKind, createFromPrometheusSource)
}

func createFromPrometheusOpSpec(args flux.Arguments, administration *flux.Administration) (flux.OperationSpec, error) {
	spec := new(FromPrometheusOpSpec) // reading flux.args and extracting params

	return spec, nil
}

func newFromPrometheusOp() flux.OperationSpec {
	return new(FromPrometheusOpSpec)
}

func (s *FromPrometheusOpSpec) Kind() flux.OperationKind {
	return FromPrometheusKind
}

type FromPrometheusProcedureSpec struct {
	plan.DefaultCost
}

func newFromPrometheusProcedure(qs flux.OperationSpec, pa plan.Administration) (plan.ProcedureSpec, error) {
	_, ok := qs.(*FromPrometheusOpSpec)
	if !ok {
		return nil, fmt.Errorf("invalid spec type %T", qs)
	}

	return &FromPrometheusProcedureSpec{}, nil
}

func (s *FromPrometheusProcedureSpec) Kind() plan.ProcedureKind {
	return FromPrometheusKind
}

func (s *FromPrometheusProcedureSpec) Copy() plan.ProcedureSpec {
	ns := new(FromPrometheusProcedureSpec)
	return ns
}

// uses a procedure spec to create a source object for flux runtime
func createFromPrometheusSource(prSpec plan.ProcedureSpec, dsid execute.DatasetID, a execute.Administration) (execute.Source, error) {
	spec, ok := prSpec.(*FromPrometheusProcedureSpec)
	if !ok {
		return nil, fmt.Errorf("invalid spec type %T", prSpec)
	}

	PrometheusIterator := PrometheusIterator{id: dsid, spec: spec, administration: a}

	return execute.CreateSourceFromDecoder(&PrometheusIterator, dsid, a)
}

type PrometheusIterator struct {
	id             execute.DatasetID
	administration execute.Administration
	spec           *FromPrometheusProcedureSpec
	reader         *execute.RowReader
}

// prometheusScraper handles parsing prometheus metrics.
// implements Scraper interfaces.
// type prometheusScraper struct{}

// ScraperTarget is a target to scrape
type ScraperTarget struct {
	Name string `json:"name"`
	// Type ScraperType `json:"type"`
	URL string `json:"url"`
} // what do we need to scrape at bare minimum?

// Metrics is the default influx based metrics.
type Metrics struct {
	Name      string                 `json:"name"`
	Tags      map[string]string      `json:"tags"`
	Fields    map[string]interface{} `json:"fields"`
	Timestamp time.Time              `json:"timestamp"`
	Type      string                 `json:"string"` // need to add other funcs for validation; first make sure this is right
}

// maybe return opSpec, Table?
func (p *PrometheusIterator) Connect(ctx context.Context, target *ScraperTarget) error {
	resp, err := http.Get(target.URL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
}

func (p *PrometheusIterator) parse(r io.Reader, header http.Header, target *ScraperTarget) (err error) {
	var parser expfmt.TextParser
	now := time.Now()

	mediatype, params, err := mime.ParseMediaType(header.Get("Content-Type"))
	if err != nil {
		return nil, err
	} // why do we need to parse the media type?

	// Prepare output
	metricFamilies := make(map[string]*dto.MetricFamily)
	if mediatype == "application/vnd.google.protobuf" &&
		// read about protobufs
		params["encoding"] == "delimited" &&
		params["proto"] == "io.prometheus.client.MetricFamily" {
		for {
			mf := &dto.MetricFamily{}
			if _, err := pbutil.ReadDelimited(r, mf); err != nil {
				if err == io.EOF {
					break
				}
				return mets, fmt.Errorf("reading metric family protocol buffer failed: %s", err)
			}
			metricFamilies[mf.GetName()] = mf
		}
	} else {
		metricFamilies, err = parser.TextToMetricFamilies(r)
		if err != nil {
			return mets, fmt.Errorf("reading text format failed: %s", err)
		}
	}
	ms := make([]Metrics, 0)

	// read metrics
	for name, family := range metricFamilies {
		for _, m := range family.Metric {
			// read tags
			tags := makeLabels(m)

			// read fields
			fields := make(map[string]interface{})

			switch family.GetType() {
			case dto.MetricType_SUMMARY:
				continue // skip for now
			case dto.MetricType_HISTOGRAM:
				continue // skip for now
			default:
				fields = getNameAndValue(m)
			}
			// fmt.Println("FIELDS", fields)
			// convert to row for flux Table
			if len(fields) > 0 {
				tm := now
				if m.TimestampMs != nil && *m.TimestampMs > 0 {
					tm = time.Unix(0, *m.TimestampMs*1000000)
				}
				met := Metrics{
					Timestamp: tm,
					Tags:      tags,
					Fields:    fields,
					Name:      name,
					// Type:      string(family.GetType()),
				}
				ms = append(ms, met)
			}

		}
	}
	return ms, nil
}

func (p *PrometheusIterator) Decode(ctx context.Context, metrics []Metrics) (table flux.Table, err error) {
	groupKey := execute.NewGroupKeyBuilder(nil)
	groupKey.AddKeyValue("tag1", values.NewString("T1"))

	for _, met := range metrics {
		fmt.Println("TIMESTAMP", met.Timestamp, "TAGS", met.Tags, "FIELDS", met.Fields, "NAME:", met.Name)
	}
	return table, nil
}

func (p *PrometheusIterator) Close() error {
	return nil
}

// Get Quantiles from summary metric
func makeQuantiles(m *dto.Metric) map[string]interface{} {
	fields := make(map[string]interface{})
	for _, q := range m.GetSummary().Quantile {
		if !math.IsNaN(q.GetValue()) {
			fields[fmt.Sprint(q.GetQuantile())] = float64(q.GetValue())
		}
	}
	return fields
}

// Get Buckets  from histogram metric
func makeBuckets(m *dto.Metric) map[string]interface{} {
	fields := make(map[string]interface{})
	for _, b := range m.GetHistogram().Bucket {
		fields[fmt.Sprint(b.GetUpperBound())] = float64(b.GetCumulativeCount())
	}
	return fields
}

// Get labels from metric
func makeLabels(m *dto.Metric) map[string]string {
	result := map[string]string{}
	for _, lp := range m.Label {
		result[lp.GetName()] = lp.GetValue()
	}
	return result
}

// Get name and value from metric
func getNameAndValue(m *dto.Metric) map[string]interface{} {
	fields := make(map[string]interface{})
	if m.Gauge != nil {
		if !math.IsNaN(m.GetGauge().GetValue()) {
			fields["gauge"] = float64(m.GetGauge().GetValue())
		}
	} else if m.Counter != nil {
		if !math.IsNaN(m.GetCounter().GetValue()) {
			fields["counter"] = float64(m.GetCounter().GetValue())
		}
	} else if m.Untyped != nil {
		if !math.IsNaN(m.GetUntyped().GetValue()) {
			fields["value"] = float64(m.GetUntyped().GetValue())
		}
	}
	return fields
}

// func main() {
// 	PrometheusIterator := PrometheusIterator{id: dsid, spec: spec, administration: a}
// 	// var url string
// 	ctx := context.Background()
// 	target := ScraperTarget{
// 		Name: "InfluxDB scraper1554449651340-IT",
// 		// Type: "prometheus",
// 		URL: "http://localhost:9090/metrics",
// 	}
// 	mets, err := scraper.Connect(ctx, &target)
// 	if err != nil {
// 		fmt.Println(err)
// 	}
// 	PrometheusIterator.Decode(ctx, mets)
// }
