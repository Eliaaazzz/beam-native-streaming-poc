#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""SDF wrapper DoFn that reads from an ``UnboundedSource``.

The DoFn owns the reader lifecycle and drives all I/O (``start`` /
``advance`` / ``get_current``).  The tracker is a pure state machine;
``try_claim(element)`` is a lightweight declaration that the DoFn is
about to yield *element*.
"""

import logging
import struct
import threading
from typing import Optional
from typing import Protocol
from typing import cast

import apache_beam as beam
from apache_beam.internal import pickler
from apache_beam.metrics import Metrics
from apache_beam.io.watermark_estimators import ManualWatermarkEstimator
from apache_beam.runners.sdf_utils import RestrictionTrackerView
from apache_beam.transforms import core
from apache_beam.transforms.core import ProcessContinuation
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils.timestamp import MAX_TIMESTAMP
from apache_beam.utils.timestamp import MIN_TIMESTAMP
from apache_beam.utils.timestamp import Timestamp

from beam_streaming_poc.unbounded_source.restriction import NOOP_CHECKPOINT_MARK
from beam_streaming_poc.unbounded_source.tracker import _SDFUnboundedSourceRestrictionProvider
from beam_streaming_poc.unbounded_source.tracker import _SDFUnboundedSourceWatermarkEstimatorProvider

LOG = logging.getLogger(__name__)
_READER_CACHE_LOCK = threading.Lock()
_READER_CACHE = {}


class _SupportsSetWatermark(Protocol):
  def set_watermark(self, watermark: Timestamp) -> None:
    pass


def _ensure_timestamp_within_bounds(value: Timestamp) -> Timestamp:
  if value < MIN_TIMESTAMP:
    return MIN_TIMESTAMP
  if value > MAX_TIMESTAMP:
    return MAX_TIMESTAMP
  return value


def _clamp_watermark(value: Timestamp, lower_bound: Timestamp) -> Timestamp:
  value = _ensure_timestamp_within_bounds(value)
  if value < lower_bound:
    return lower_bound
  return value


def _create_reader_cache_key(source, checkpoint) -> bytes:
  source_bytes = pickler.dumps(source)
  if checkpoint is None:
    checkpoint_bytes = b''
    has_checkpoint = False
  else:
    checkpoint_bytes = source.get_checkpoint_mark_coder().encode(checkpoint)
    has_checkpoint = True
  return (
      struct.pack(
          '>BII', int(has_checkpoint), len(source_bytes), len(checkpoint_bytes)) +
      source_bytes + checkpoint_bytes)


def _pop_cached_reader(source, checkpoint):
  """Pop a cached (reader, is_started) pair for the given source/checkpoint."""
  cache_key = _create_reader_cache_key(source, checkpoint)
  with _READER_CACHE_LOCK:
    return _READER_CACHE.pop(cache_key, None)


def _put_cached_reader(source, checkpoint, reader, is_started) -> None:
  """Cache a reader for reuse when the residual is scheduled on the same worker."""
  cache_key = _create_reader_cache_key(source, checkpoint)
  previous = None
  with _READER_CACHE_LOCK:
    previous = _READER_CACHE.get(cache_key)
    _READER_CACHE[cache_key] = (reader, is_started)
  if previous is not None and previous[0] is not reader:
    try:
      previous[0].close()
    except Exception:
      LOG.warning('Failed to close replaced cached UnboundedReader.',
                  exc_info=True)


def _register_finalizer(bundle_finalizer, checkpoint) -> None:
  """Register checkpoint finalization if the checkpoint is non-trivial."""
  if checkpoint is not None and checkpoint is not NOOP_CHECKPOINT_MARK:
    bundle_finalizer.register(checkpoint.finalize_checkpoint)


class _SDFUnboundedSourceDoFn(core.DoFn):
  """SDF DoFn that reads from an ``UnboundedSource``.

  The DoFn owns the reader lifecycle and drives all I/O.  The tracker is
  a pure state machine; ``try_claim(element)`` is a lightweight declaration.

  Reader caching mirrors Java's ``cachedReaders`` map in
  ``UnboundedSourceAsSDFWrapperFn``.
  """
  POLL_INTERVAL_SECS = 1.0

  def setup(self):
    self._pipeline_options = None

  @core.DoFn.unbounded_per_element()
  def process(
      self,
      source,
      tracker=core.DoFn.RestrictionParam(
          _SDFUnboundedSourceRestrictionProvider()),
      watermark_estimator=core.DoFn.WatermarkEstimatorParam(
          _SDFUnboundedSourceWatermarkEstimatorProvider()),
      bundle_finalizer=core.DoFn.BundleFinalizerParam()):
    tracker_view = cast(RestrictionTrackerView, tracker)
    wm_estimator = cast(_SupportsSetWatermark, watermark_estimator)
    restriction = tracker_view.current_restriction()

    # ── Reader acquisition ──────────────────────────────────────────
    cached = _pop_cached_reader(source, restriction.checkpoint)
    if cached is not None:
      reader, reader_started = cached
      Metrics.counter('UnboundedSourceSDF', 'reader_reuse_count').inc()
    else:
      reader = source.create_reader(
          self._pipeline_options, restriction.checkpoint)
      reader_started = False
      Metrics.counter('UnboundedSourceSDF', 'reader_create_count').inc()

    # ── Read loop ───────────────────────────────────────────────────
    has_data = reader.start() if not reader_started else reader.advance()

    while True:
      if has_data:
        checkpoint = reader.get_checkpoint_mark()
        wm = _clamp_watermark(reader.get_watermark(), restriction.watermark)
        restriction.checkpoint = checkpoint
        restriction.watermark = wm

        if not tracker_view.try_claim(reader.get_current()):
          _put_cached_reader(source, checkpoint, reader, True)
          _register_finalizer(bundle_finalizer, checkpoint)
          return

        wm_estimator.set_watermark(wm)
        yield TimestampedValue(reader.get_current(), reader.get_current_timestamp())

        if wm == MAX_TIMESTAMP:
          restriction.is_done = True
          reader.close()
          _register_finalizer(bundle_finalizer, checkpoint)
          return

        if tracker_view.current_restriction().is_done:
          _put_cached_reader(source, checkpoint, reader, True)
          _register_finalizer(bundle_finalizer, checkpoint)
          return

      else:
        checkpoint = reader.get_checkpoint_mark()
        wm = _clamp_watermark(reader.get_watermark(), restriction.watermark)
        restriction.checkpoint = checkpoint
        restriction.watermark = wm
        wm_estimator.set_watermark(wm)

        if wm == MAX_TIMESTAMP:
          restriction.is_done = True
          reader.close()
          _register_finalizer(bundle_finalizer, checkpoint)
          return

        _put_cached_reader(source, checkpoint, reader, True)
        _register_finalizer(bundle_finalizer, checkpoint)
        yield ProcessContinuation.resume(self.POLL_INTERVAL_SECS)
        return

      has_data = reader.advance()


class ReadFromUnboundedSourceFn(beam.PTransform):
  """PTransform that reads from each ``UnboundedSource`` via SDF.

  Usage::

      p | beam.Create([my_unbounded_source]) | ReadFromUnboundedSourceFn()
  """
  def expand(self, input_or_inputs):
    return input_or_inputs | core.ParDo(_SDFUnboundedSourceDoFn())
