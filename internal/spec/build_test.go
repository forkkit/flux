package spec_test

import (
	"context"
	"testing"
	"time"

	_ "github.com/influxdata/flux/builtin"
	"github.com/influxdata/flux/dependencies/dependenciestest"
	"github.com/influxdata/flux/internal/spec"
)

func Benchmark_Build(b *testing.B) {
	query := `
import "influxdata/influxdb/monitor"
import "slack"
import "influxdata/influxdb/secrets"
import "experimental"

option task = {name: "Any", every: 1m}

slack_endpoint = slack.endpoint(url: "alsdk")
notification = {
	_notification_rule_id: "",
	_notification_rule_name: "Any",
	_notification_endpoint_id: "isldsl",
	_notification_endpoint_name: "Doc Team Slack Webhook",
}
statuses = monitor.from(start: -2m, fn: (r) =>
	(r.use_case == "2"))
ok_to_any = statuses
	|> monitor.stateChanges(fromLevel: "ok", toLevel: "any")
all_statuses = ok_to_any
	|> filter(fn: (r) =>
		(r._time > experimental.subDuration(from: now(), d: 1m)))

all_statuses
	|> monitor.notify(data: notification, endpoint: slack_endpoint(mapFn: (r) =>
		({channel: "", text: "Checking on ANY", color: if r._level == "crit" then "danger" else if r._level == "warn" then "warning" else "good"})))
`
	ctx := context.Background()
	deps := dependenciestest.Default()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := spec.FromScript(ctx, deps, time.Now(), query); err != nil {
			b.Fatal(err)
		}
	}
}
