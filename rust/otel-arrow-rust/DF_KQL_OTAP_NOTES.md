
# Background for requirements:

### References

**OTTL**
- Functions https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/pkg/ottl/ottlfuncs#ottl-functions
- Grammar https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/pkg/ottl/LANGUAGE.md
- 


### Scopes/contexts:
For each type here, there is a bunch of contexts:

| Telemetry               | OTTL Context                                                                                                                               |
|-------------------------|--------------------------------------------------------------------------------------------------------------------------------------------|
| `Resource`              | [Resource](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/pkg/ottl/contexts/ottlresource/README.md)           |
| `Instrumentation Scope` | [Instrumentation Scope](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/pkg/ottl/contexts/ottlscope/README.md) |
| `Span`                  | [Span](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/pkg/ottl/contexts/ottlspan/README.md)                   |
| `Span Event`            | [SpanEvent](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/pkg/ottl/contexts/ottlspanevent/README.md)         |
| `Metric`                | [Metric](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/pkg/ottl/contexts/ottlmetric/README.md)               |
| `Datapoint`             | [DataPoint](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/pkg/ottl/contexts/ottldatapoint/README.md)         |
| `Log`                   | [Log](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/pkg/ottl/contexts/ottllog/README.md)                     |
| `Profile`               | [Profile](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/pkg/ottl/contexts/ottlprofile/README.md)             |

Weird things
- span.trace_id.string = trace_id as a string
- span.cache[""] - a secret map of anyvalues?
- 

### Low Level functions:

**Editors**

Not sure the target?
- `append(target, Optional[value], Optional[values])`

Target could be not map:
- `replace_match(target, pattern, replacement, Optional[function], Optional[replacementFormat])` - The replace_match function allows replacing entire strings if they match a glob pattern.
- `replace_pattern(target, regex, replacement, Optional[function], Optional[replacementFormat])` - The replace_pattern function allows replacing all string sections that match a regex pattern with a new value.
- `set(target, value)` - The set function allows users to set a telemetry field using a value.

Target is map:
- `delete_key(target, key)` The delete_key function removes a key from a pcommon.Map
- `delete_matching_keys(target, pattern)` - The delete_matching_keys function removes all keys from a pcommon.Map that match a regex pattern.
- `flatten(target, Optional[prefix], Optional[depth], Optional[resolveConflicts])` - The flatten function flattens a pcommon.Map by moving items from nested maps to the root.
- `keep_keys(target, keys[])` - The keep_keys function removes all keys from the pcommon.Map that do not match one of the supplied keys.
- `keep_matching_keys(target, pattern)` - The keep_matching_keys function keeps all keys from a pcommon.Map that match a regex pattern.
- `limit(target, limit, priority_keys[])` - The limit function reduces the number of elements in a pcommon.Map to be no greater than the limit.
- `merge_maps(target, source, strategy)` - The merge_maps function merges the source map into the target map using the supplied strategy to handle conflicts.
- `replace_all_matches(target, pattern, replacement, Optional[function], Optional[replacementFormat])` The replace_all_matches function replaces any matching string value with the replacement string.
- `replace_all_patterns` replace_all_patterns(target, mode, regex, replacement, Optional[function], Optional[replacementFormat])
- `truncate_all(target, limit)` The truncate_all function truncates all string values in a pcommon.Map so that none are longer than the limit.

**Converters**
- there are many many


### Operations

These are the kinds of operations users are generally performing on Telemetry data:

**Filtering**

Here we're identifying data by some pattern by some pattern, and dropping it ...

For example, see [Filter Processor](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/processor/filterprocessor#configuration)

> The filterprocessor utilizes the OpenTelemetry Transformation Language to create conditions that determine when telemetry should be dropped. If any condition is met, the telemetry is dropped (each condition is ORed together). 

Filtering can occur on multiple telemetry contexts:

| Config              | OTTL Context                                                                                                                       |
|---------------------|------------------------------------------------------------------------------------------------------------------------------------|
| `traces.span`       | [Span](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/pkg/ottl/contexts/ottlspan/README.md)           |
| `traces.spanevent`  | [SpanEvent](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/pkg/ottl/contexts/ottlspanevent/README.md) |
| `metrics.metric`    | [Metric](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/pkg/ottl/contexts/ottlmetric/README.md)       |
| `metrics.datapoint` | [DataPoint](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/pkg/ottl/contexts/ottldatapoint/README.md) |
| `logs.log_record`   | [Log](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/pkg/ottl/contexts/ottllog/README.md)             |


```
processors:
  filter/ottl:
    error_mode: ignore
    traces:
      span:
        - 'attributes["container.name"] == "app_container_1"'
        - 'resource.attributes["host.name"] == "localhost"'
        - 'name == "app_3"'
      spanevent:
        - 'attributes["grpc"] == true'
        - 'IsMatch(name, ".*grpc.*")'
    metrics:
      metric:
          - 'name == "my.metric" and resource.attributes["my_label"] == "abc123"'
          - 'type == METRIC_DATA_TYPE_HISTOGRAM'
      datapoint:
          - 'metric.type == METRIC_DATA_TYPE_SUMMARY'
          - 'resource.attributes["service.name"] == "my_service_name"'
    logs:
      log_record:
        - 'IsMatch(body, ".*password.*")'
        - 'severity_number < SEVERITY_NUMBER_WARN'
```

**Routing**

Here we're identifying data by some pattern, and forwarding it in it's entirety

For example in [Router Processor](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/connector/routingconnector/README.md)


**Transformation**

[Transform processor](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/processor/transformprocessor)

Within each `<signal_statements>` list, only certain OTTL Path prefixes can be used:

| Signal             | Path Prefix Values                             |
|--------------------|------------------------------------------------|
| trace_statements   | `resource`, `scope`, `span`, and `spanevent`   |
| metric_statements  | `resource`, `scope`, `metric`, and `datapoint` |
| log_statements     | `resource`, `scope`, and `log`                 |
| profile_statements | `resource`, `scope`, and `profile`             |

_Q: DO WE NEED TO VALIDATE THE STATEMENTS?_
>`error_mode`: determines how the processor treats errors that occur while processing a statement. If the top-level error_mode is not specified, propagate will be used. The top-level error_mode can be overridden at statement group level, offering more granular control over error handling. If the statement group error_mode is not specified, the top-level error_mode is applied.

basic config: identify & transform in one shot:
```
  log_statements:
    - set(log.severity_text, "FAIL") where log.body == "request failed"
    - replace_all_matches(log.attributes, "/user/*/list/*", "/user/{userId}/list/{listId}")
    - replace_all_patterns(log.attributes, "value", "/account/\\d{4}", "/account/{accountId}")
    - set(log.body, log.attributes["http.route"])
```

advanced config: identify & make multiple transformations:
```
  log_statements:
    - conditions:
        - IsMap(log.body) and log.body["object"] != nil
      statements:
        - set(log.body, log.attributes["http.route"])
```