### join

`join` merges two input streams into a single output stream.
Records that have the same group key and `_time` values will be joined in the output.
The output groups will be the same as the input groups.

`join` has the following properties:

| Name    | Type     | Description |
| ------- | -------- | ----------- |
| tables  | object   | Specifies the two table streams to join. |
| fn      | function | Defines the function that joins the rows of each table. The return value is an object which defines the output record structure. This function must preserve the table grouping. |

Ex.
```
import "internal/promql"

left = from(bucket: "my-bucket")
    |> range(start: -1h)
    |> filter(fn: (r) => r._field == "A")
    |> drop(columns: ["_field"])
    |> rename(columns: {_value: "a"})

right = from(bucket: "my-bucket")
    |> range(start: -1h)
    |> filter(fn: (r) => r._field == "B")
    |> drop(columns: ["_field"])
    |> rename(columns: {_value: "b"})

promql.join(tables: {left, right}, fn: (t) => {
    return {t.left with b: t.right.b}
}
```

`join` assumes that each input table is sorted by `_time`.
The planner will ensure this by inserting a `sort` procedure after each one of join's immediate predecessors.
`join` will only call `fn` on rows that pass the join condition.

Here are some examples of compiling PromQL queries into their equivalent Flux forms involving `promql.join`.

```
left_metric + right_metric
```
compiles to:
```
lhs = left_metric
    |> rename(columns: {_value: "lv"})

rhs = right_metric
    |> rename(columns: {_value: "rv"})

promql.join(tables: {lhs, rhs}, fn: (t) => ({t.lhs with rv: t.rv}))
    |> map(fn: (r) => ({r with _value: r.lv + r.rv}))
    |> drop(columns: ["lv", "rv"])
```

```
left_metric + on(tagA,tagB) right_metric
```
compiles to:
```
lhs = left_metric
    |> rename(columns: {_value: "lv"})
    |> group(columns: ["tagA", "tagB"])

rhs = right_metric
    |> rename(columns: {_value: "rv"})
    |> group(columns: ["tagA", "tagB"])

promql.join(tables: {lhs, rhs}, fn: (t) => ({t.lhs with rv: t.rv}))
    |> map(fn: (r) => ({r with _value: r.lv + r.rv}))
    |> drop(columns: ["lv", "rv"])
```

```
left_metric + ignore(tagA,tagB) right_metric
```
compiles to:
```
lhs = left_metric
    |> rename(columns: {_value: "lv"})
    |> group(columns: ["tagA", "tagB", "_time", "lv"], mode: "except")

rhs = right_metric
    |> rename(columns: {_value: "rv"})
    |> group(columns: ["tagA", "tagB", "_time", "rv"], mode: "except")

promql.join(tables: {lhs, rhs}, fn: (t) => ({t.lhs with rv: t.rv}))
    |> map(fn: (r) => ({r with _value: r.lv + r.rv}))
    |> drop(columns: ["lv", "rv"])
```