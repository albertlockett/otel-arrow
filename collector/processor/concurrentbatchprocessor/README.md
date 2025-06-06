# Batch Processor

**Component Status: Removed. The exporterhelper batcher is mature,
this component is no longer needed.**

The batch processor accepts spans, metrics, or logs and places them into
batches. Batching helps better compress the data and reduce the number of
outgoing connections required to transmit the data. This processor supports both
size and time based batching.

It is highly recommended to configure the batch processor on every collector.
The batch processor should be defined in the pipeline after the `memory_limiter`
as well as any sampling processors. This is because batching should happen after
any data drops such as sampling.

Please refer to [config.go](./config.go) for the config spec.

The following configuration options can be modified:

- `send_batch_size` (default = 8192): Number of spans, metric data points, or
log records after which a batch will be sent regardless of the timeout.
`send_batch_size` acts as a trigger and does not affect the size of the batch.
If you need to enforce batch size limits sent to the next component in the
pipeline see `send_batch_max_size`.
- `timeout` (default = 200ms): Time duration after which a batch will be sent
regardless of size.  If set to zero, `send_batch_size` is ignored as data will
be sent immediately, subject to only `send_batch_max_size`.
- `send_batch_max_size` (default = 0): The upper limit of the batch size. `0`
  means no upper limit of the batch size. This property ensures that larger
  batches are split into smaller units. It must be greater than or equal to
  `send_batch_size`.
- `metadata_keys` (default = empty): When set, this processor will create one
  batcher instance per distinct combination of values in the `client.Metadata`.
- `metadata_cardinality_limit` (default = 1000): When `metadata_keys` is not
  empty, this setting limits the number of unique combinations of metadata key
  values that will be processed over the lifetime of the process.
- `early_return` (default = false): When enabled, this pipeline component will
  return immediate success to the caller after enqueuing the item for eventual
  delivery.
- `max_concurrency` (default = unlimited): Controls the maximum number of
  concurrent export calls made by this component.  This is enforced per batcher
  instance, as determined by `metadata_keys`.  When the value 0 is configured,
  unlimited concurrency is allowed.

See notes about metadata batching below.

Examples:

This configuration contains one default batch processor and a second with custom
settings.  The `batch/2` processor will buffer up to 10000 spans, metric data
points, or log records for up to 10 seconds without splitting data items to
enforce a maximum batch size.

```yaml
processors:
  batch:
  batch/2:
    send_batch_size: 10000
    timeout: 10s
```

This configuration will enforce a maximum batch size limit of 10000 spans,
metric data points, or log records without introducing any artificial delays.

```yaml
processors:
  batch:
    send_batch_max_size: 10000
    timeout: 0s
```

Refer to [config.yaml](./testdata/config.yaml) for detailed examples on using
the processor.

## Batching and client metadata

Batching by metadata enables support for multi-tenant OpenTelemetry Collector
pipelines with batching over groups of data having the same authorization
metadata.  For example:

```yaml
processors:
  batch:
    # batch data by tenant-id
    metadata_keys:
    - tenant_id

    # limit to 10 batcher processes before raising errors
    metadata_cardinality_limit: 10
```

Receivers should be configured with `include_metadata: true` so that metadata
keys are available to the processor.

Note that each distinct combination of metadata triggers the allocation of a new
background task in the Collector that runs for the lifetime of the process, and
each background task holds one pending batch of up to `send_batch_size` records.
Batching by metadata can therefore substantially increase the amount of memory
dedicated to batching.

The maximum number of distinct combinations is limited to the configured
`metadata_cardinality_limit`, which defaults to 1000 to limit memory impact.

Users of the batching processor configured with metadata keys should consider
use of an Auth extension to validate the relevant metadata-key values.

The number of batch processors currently in use is exported as the
`otelcol_processor_batch_metadata_cardinality` metric.

## Batching with error transmission

The use of unlimited concurrency is recommended for this component.

This component's legacy configuration had `max_concurrency` of 1 and
`early_return` set true.  The use of `early_return` in the legacy configuration
prevented error transmission through this component.

When the exporterhelper `queue_sender` is disabled, which is also necessary for
error transmission, the result combined with `max_concurrency` of 1 would be
synchronous export behavior, meaning that a new batch could not be formed until
the preceding batch completed its export.  Setting `max_concurrency` to 0 for
unlimited concurrency is recommended because it works with all configurations of
the exporterhelper.

The use of unlimited concurrency should not be considered a risk, because the
actions of this processor take place after the associated memory has been
allocated.  Users are expected to implement memory limits using other means,
possibly via the `memorylimiter` extension or another form of admission control.
