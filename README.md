# Apache Beam Python SDK Streaming Transforms PoC

This repository contains a standalone proof of concept for two missing Python
SDK streaming primitives:

- `UnboundedSource` wrapped as a Splittable DoFn.
- `Watch.growth_of(...)` implemented as a polling SDF.

The work targets [BEAM-19137](https://issues.apache.org/jira/browse/BEAM-19137)
and [BEAM-21521](https://issues.apache.org/jira/browse/BEAM-21521). It is meant
for design validation and review, not production use.

## Quick Start

```bash
git clone https://github.com/Eliaaazzz/beam-native-streaming-poc.git
cd beam-native-streaming-poc

uv venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
uv pip install -e ".[dev]"

pytest tests/ -v
```

## Usage

### UnboundedSource as SDF

```python
import apache_beam as beam
from beam_streaming_poc.unbounded_source import ReadFromUnboundedSourceFn

with beam.Pipeline(options=...) as p:
  records = (
      p
      | beam.Create([my_unbounded_source])
      | ReadFromUnboundedSourceFn()
  )
```

Runnable example: [`examples/unbounded_source_example.py`](examples/unbounded_source_example.py)

### Watch Growth

```python
import apache_beam as beam
from beam_streaming_poc.watch import PollResult, Watch

def poll_fn(seed):
  values = query_external_system(seed)
  return PollResult.of(values, complete=False)

with beam.Pipeline(options=...) as p:
  out = (
      p
      | beam.Create(["seed-1", "seed-2"])
      | Watch.growth_of(poll_fn).with_poll_interval(1.0)
  )
```

Runnable example: [`examples/watch_growth_example.py`](examples/watch_growth_example.py)

## Design Notes

### UnboundedSource SDF Wrapper

- Restriction state carries checkpoint and watermark.
- The tracker is a pure state machine; the DoFn owns reader I/O.
- Checkpoints round-trip through a deterministic coder.
- Watermarks are propagated through the watermark estimator and clamped
  monotonically.
- Residual work resumes from persisted checkpoints.

### Watch Transform

- `PollResult` and `PollFn` mirror the Java Watch API shape.
- Growth state tracks seen element hashes, pending outputs, and completion.
- Deduplication uses stable `blake2b` hashes.
- Termination conditions are composable.
- `try_split` supports pending-output and completed-state cases.

## Java Mapping

| Python PoC | Java SDK Reference |
|---|---|
| `UnboundedSourceRestriction` | `UnboundedSourceRestriction<OutputT, CheckpointT>` |
| `UnboundedSourceRestrictionTracker` | `UnboundedSourceRestrictionTracker<OutputT, CheckpointT>` |
| `ReadFromUnboundedSourceFn` | `Read.Unbounded` via `UnboundedReadFromBoundedSource` |
| `WatchGrowthFn` | `Watch.Growth` / `Watch.GrowthFn` |
| `GrowthState` | `Watch.GrowthState` |
| `PollResult` / `PollFn` | `Watch.Growth.PollResult` / `Watch.Growth.PollFn` |
| `TerminationCondition` | `Watch.Growth.TerminationCondition` |

## Project Layout

```text
beam-native-streaming-poc/
|-- .gitignore
|-- LICENSE
|-- README.md
|-- pyproject.toml
|-- docs/
|   `-- beam-integration.md
|-- examples/
|   |-- unbounded_source_example.py
|   `-- watch_growth_example.py
|-- src/
|   `-- beam_streaming_poc/
|       |-- __init__.py
|       |-- unbounded_source/
|       |   |-- __init__.py
|       |   |-- empty_source.py
|       |   |-- restriction.py
|       |   |-- tracker.py
|       |   `-- wrapper.py
|       `-- watch/
|           |-- __init__.py
|           |-- growth_fn.py
|           |-- growth_state.py
|           |-- poll.py
|           `-- termination.py
`-- tests/
    |-- __init__.py
    |-- test_unbounded_source.py
    `-- test_watch.py
```

## Relationship to Beam Integration

This repository stays standalone. Beam-side wiring and submodule-based review
flow live in a separate integration workspace. See
[`docs/beam-integration.md`](docs/beam-integration.md) for the split rationale.

## Limitations

- In-memory state only.
- Watermarks are clamped to avoid regression.
- API shape may change during upstream review.

## License

Apache-2.0
