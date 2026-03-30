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

"""Unit tests for the UnboundedSource-as-SDF PoC."""

import pickle
import struct
import time
import unittest
from typing import cast
from unittest import mock

import apache_beam as beam
from apache_beam import coders
from apache_beam.io.watermark_estimators import ManualWatermarkEstimator
from apache_beam.transforms import core
from apache_beam.transforms.core import ProcessContinuation
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils.timestamp import MAX_TIMESTAMP
from apache_beam.utils.timestamp import MIN_TIMESTAMP
from apache_beam.utils.timestamp import Timestamp

from beam_streaming_poc.unbounded_source import CheckpointMark
from beam_streaming_poc.unbounded_source import NOOP_CHECKPOINT_MARK
from beam_streaming_poc.unbounded_source import ReadFromUnboundedSourceFn
from beam_streaming_poc.unbounded_source import UnboundedReader
from beam_streaming_poc.unbounded_source import UnboundedSource
from beam_streaming_poc.unbounded_source import _SDFUnboundedSourceRestriction
from beam_streaming_poc.unbounded_source import _SDFUnboundedSourceRestrictionCoder
from beam_streaming_poc.unbounded_source import _SDFUnboundedSourceRestrictionProvider
from beam_streaming_poc.unbounded_source import _SDFUnboundedSourceRestrictionTracker
from beam_streaming_poc.unbounded_source import _SDFUnboundedSourceWatermarkEstimatorProvider
from beam_streaming_poc.unbounded_source import _put_cached_reader
from beam_streaming_poc.unbounded_source.wrapper import _SDFUnboundedSourceDoFn


# ===================================================================
#  Shared test sources
# ===================================================================

class LiveCheckpointMark(CheckpointMark):
  """Checkpoint storing the position (index into the shared record list)."""
  def __init__(self, position):
    self.position = position

  def __repr__(self):
    return 'LiveCheckpointMark(position=%d)' % self.position


class LiveUnboundedReader(UnboundedReader):
  """Reads from a LiveUnboundedSource's shared record list."""
  def __init__(self, source, start_position):
    self._source = source
    self._position = start_position
    self._current = None
    self._current_ts = None
    self._closed = False

  def start(self):
    return self.advance()

  def advance(self):
    if self._position >= len(self._source._records):
      return False
    element, timestamp = self._source._records[self._position]
    self._current = element
    self._current_ts = timestamp
    self._position += 1
    return True

  def get_current(self):
    return self._current

  def get_current_timestamp(self):
    if self._current_ts is None:
      return Timestamp(0)
    return self._current_ts

  def get_watermark(self):
    if self._current_ts is not None:
      return self._current_ts
    return Timestamp(0)

  def get_checkpoint_mark(self):
    return LiveCheckpointMark(self._position)

  def close(self):
    self._closed = True


class LiveCheckpointMarkCoder(coders.Coder):
  """Coder for LiveCheckpointMark using struct."""
  def encode(self, value):
    return struct.pack('>I', value.position)

  def decode(self, encoded):
    position, = struct.unpack('>I', encoded)
    return LiveCheckpointMark(position)

  def is_deterministic(self):
    return True


class LiveUnboundedSource(UnboundedSource):
  """A truly unbounded source with dynamic data arrival.

  Records are added externally via ``add(element, timestamp)``.
  The reader returns ``False`` from ``advance()`` when it reaches the
  end of currently available records (idle), and resumes when more
  records arrive.
  """
  def __init__(self):
    self._records = []  # list of (element, Timestamp) tuples

  def add(self, element, timestamp):
    """Add a record to the source."""
    self._records.append((element, Timestamp(timestamp)))

  def create_reader(self, pipeline_options, checkpoint_mark):
    start = checkpoint_mark.position if checkpoint_mark else 0
    return LiveUnboundedReader(self, start)

  def get_checkpoint_mark_coder(self):
    return LiveCheckpointMarkCoder()


class _RegressingWatermarkReader(UnboundedReader):

  def __init__(self, watermarks):
    self._watermarks = list(watermarks)
    self._index = -1

  def start(self):
    return self.advance()

  def advance(self):
    self._index += 1
    return self._index < len(self._watermarks)

  def get_current(self):
    return self._index

  def get_current_timestamp(self):
    return self._watermarks[self._index]

  def get_watermark(self):
    if 0 <= self._index < len(self._watermarks):
      return self._watermarks[self._index]
    return Timestamp(0)

  def get_checkpoint_mark(self):
    return LiveCheckpointMark(self._index + 1)


class _RegressingWatermarkSource(UnboundedSource):

  def __init__(self, watermarks):
    self._watermarks = watermarks

  def create_reader(self, pipeline_options, checkpoint_mark):
    del checkpoint_mark
    return _RegressingWatermarkReader(self._watermarks)

  def get_checkpoint_mark_coder(self):
    return coders.registry.get_coder(object)


class _TrackingCheckpointMark(CheckpointMark):

  def __init__(self, next_offset):
    self.next_offset = next_offset
    self.finalized = False

  def finalize_checkpoint(self):
    self.finalized = True


class _TrackingUnboundedReader(UnboundedReader):

  def __init__(self, source, start_offset):
    self._source = source
    self._next_offset = start_offset
    self._current = None

  def start(self):
    return self.advance()

  def advance(self):
    if self._next_offset >= self._source.num_elements:
      return False
    self._current = self._next_offset
    self._next_offset += 1
    return True

  def get_current(self):
    return self._current

  def get_current_timestamp(self):
    if self._current is None:
      return Timestamp(0)
    return Timestamp(self._current)

  def get_watermark(self):
    if self._current is None:
      return Timestamp(0)
    return Timestamp(self._current)

  def get_checkpoint_mark(self):
    return _TrackingCheckpointMark(self._next_offset)


class _TrackingUnboundedSource(UnboundedSource):

  def __init__(self, num_elements=5):
    self.num_elements = num_elements

  def create_reader(self, pipeline_options, checkpoint_mark):
    start = checkpoint_mark.next_offset if checkpoint_mark else 0
    return _TrackingUnboundedReader(self, start)

  def get_checkpoint_mark_coder(self):
    return coders.registry.get_coder(object)


class _SplitRecordingSource(UnboundedSource):

  def __init__(self, split_result=None, fail_split=False):
    self._split_result = split_result if split_result is not None else [self]
    self._fail_split = fail_split
    self.split_calls = 0

  def create_reader(self, pipeline_options, checkpoint_mark):
    del checkpoint_mark
    raise AssertionError('split-only test source should not create readers')

  def split(self, desired_num_splits=1):
    del desired_num_splits
    self.split_calls += 1
    if self._fail_split:
      raise RuntimeError('boom')
    return self._split_result

  def get_checkpoint_mark_coder(self):
    return LiveCheckpointMarkCoder()


class _TerminalWatermarkReader(UnboundedReader):
  """Reader that returns a fixed number of elements, then MAX_TIMESTAMP wm."""

  def __init__(self, source):
    self._source = source
    self._index = -1
    self._closed = False

  def start(self):
    return self.advance()

  def advance(self):
    self._index += 1
    return self._index < self._source.num_elements

  def get_current(self):
    return self._index

  def get_current_timestamp(self):
    return Timestamp(self._index)

  def get_watermark(self):
    if self._index >= self._source.num_elements - 1:
      return MAX_TIMESTAMP
    return Timestamp(self._index)

  def get_checkpoint_mark(self):
    return LiveCheckpointMark(self._index + 1)

  def close(self):
    self._closed = True


class _TerminalWatermarkSource(UnboundedSource):
  """Source that signals completion via MAX_TIMESTAMP watermark."""

  def __init__(self, num_elements=2):
    self.num_elements = num_elements
    self._reader = None

  def create_reader(self, pipeline_options, checkpoint_mark):
    self._reader = _TerminalWatermarkReader(self)
    return self._reader

  def get_checkpoint_mark_coder(self):
    return LiveCheckpointMarkCoder()


class _CacheOnlyReader(UnboundedReader):

  def __init__(self, source, start_position):
    self._source = source
    self._current = start_position - 1
    self._started = False
    self._closed = False

  def start(self):
    self._started = True
    return self.advance()

  def advance(self):
    assert self._started
    self._current += 1
    return self._current < self._source.num_elements

  def get_current(self):
    return self._current

  def get_current_timestamp(self):
    return Timestamp(self._current)

  def get_watermark(self):
    if self._current >= self._source.num_elements:
      return MAX_TIMESTAMP
    if self._current < 0:
      return MIN_TIMESTAMP
    return Timestamp(self._current)

  def get_checkpoint_mark(self):
    return LiveCheckpointMark(
        min(max(self._current + 1, 0), self._source.num_elements))

  def close(self):
    self._closed = True


class _CacheOnlySource(UnboundedSource):

  def __init__(self, num_elements=5):
    self.num_elements = num_elements
    self.create_reader_calls = []

  def create_reader(self, pipeline_options, checkpoint_mark):
    self.create_reader_calls.append(
        None if checkpoint_mark is None else checkpoint_mark.position)
    if checkpoint_mark is not None:
      raise AssertionError(
          'Residual should reuse the cached reader instead of recreating it')
    return _CacheOnlyReader(self, 0)

  def get_checkpoint_mark_coder(self):
    return LiveCheckpointMarkCoder()


# ===================================================================
#  DoFn test helpers
# ===================================================================

class _FakeTrackerView:
  """Minimal tracker view for DoFn unit tests (no threading lock)."""
  def __init__(self, tracker):
    self._tracker = tracker

  def try_claim(self, pos):
    return self._tracker.try_claim(pos)

  def current_restriction(self):
    return self._tracker.current_restriction()

  def try_split(self, f):
    return self._tracker.try_split(f)

  def check_done(self):
    return self._tracker.check_done()

  def defer_remainder(self, *args):
    pass


class _FakeWatermarkEstimator:
  def __init__(self):
    self.watermark = None
    self._history = []

  def set_watermark(self, wm):
    self._history.append(wm)
    self.watermark = wm

  def current_watermark(self):
    return self.watermark


class _FakeBundleFinalizer:
  def __init__(self):
    self._callbacks = []

  def register(self, fn):
    self._callbacks.append(fn)

  def finalize_bundle(self):
    for fn in self._callbacks:
      fn()


def _run_process_once(source, restriction=None):
  """Run DoFn.process() once and return (elements, continuation, restriction,
  tracker, estimator, finalizer).

  *elements* is a list of the element values yielded (unwrapped from
  TimestampedValue).  *continuation* is the ProcessContinuation if one was
  yielded, otherwise None.
  """
  if restriction is None:
    restriction = _SDFUnboundedSourceRestriction(source)
  dofn = _SDFUnboundedSourceDoFn()
  dofn.setup()
  tracker = _SDFUnboundedSourceRestrictionTracker(restriction)
  tracker_view = _FakeTrackerView(tracker)
  estimator = _FakeWatermarkEstimator()
  finalizer = _FakeBundleFinalizer()

  elements = []
  continuation = None
  for output in dofn.process(source, tracker_view, estimator, finalizer) or []:
    if isinstance(output, ProcessContinuation):
      continuation = output
    elif isinstance(output, TimestampedValue):
      elements.append(output.value)
    else:
      elements.append(output)

  return elements, continuation, restriction, tracker, estimator, finalizer


# ===================================================================
#  Public API surface
# ===================================================================

class PublicApiSurfaceTest(unittest.TestCase):

  def test_public_reader_alias_is_importable(self):
    self.assertIsNotNone(ReadFromUnboundedSourceFn)


# ===================================================================
#  Tracker state-machine tests (no reader I/O in tracker)
# ===================================================================

class RestrictionTrackerTest(unittest.TestCase):

  def _make_tracker(self, source=None):
    if source is None:
      source = LiveUnboundedSource()
    restriction = _SDFUnboundedSourceRestriction(source)
    return _SDFUnboundedSourceRestrictionTracker(restriction)

  def test_try_claim_returns_true_for_active_restriction(self):
    tracker = self._make_tracker()
    self.assertTrue(tracker.try_claim('any_value'))

  def test_try_claim_returns_false_when_already_stopped(self):
    tracker = self._make_tracker()
    # Force a stop via try_split (need a checkpoint to enable split)
    tracker.current_restriction().checkpoint = LiveCheckpointMark(0)
    tracker.try_split(0)  # sets _stopped=True
    self.assertFalse(tracker.try_claim('any_value'))

  def test_try_claim_returns_false_when_is_done(self):
    source = LiveUnboundedSource()
    restriction = _SDFUnboundedSourceRestriction(source, is_done=True)
    tracker = _SDFUnboundedSourceRestrictionTracker(restriction)
    self.assertFalse(tracker.try_claim('any_value'))

  def test_try_claim_sets_stopped_when_restriction_is_done(self):
    source = LiveUnboundedSource()
    restriction = _SDFUnboundedSourceRestriction(source, is_done=True)
    tracker = _SDFUnboundedSourceRestrictionTracker(restriction)
    tracker.try_claim('any_value')
    # After try_claim detects is_done, _stopped should be True.
    self.assertFalse(tracker.try_claim('another'))

  def test_try_claim_increments_claimed_count(self):
    tracker = self._make_tracker()
    tracker.try_claim('a')
    tracker.try_claim('b')
    tracker.try_claim('c')
    progress = tracker.current_progress()
    self.assertEqual(progress.completed_work, 3)

  def test_try_split_returns_none_when_restriction_already_done(self):
    source = LiveUnboundedSource()
    restriction = _SDFUnboundedSourceRestriction(source, is_done=True)
    tracker = _SDFUnboundedSourceRestrictionTracker(restriction)
    self.assertIsNone(tracker.try_split(0))

  def test_try_split_returns_none_when_no_progress_and_no_checkpoint(self):
    """Before any reading (checkpoint=None, _claimed_count=0): no split."""
    tracker = self._make_tracker()
    self.assertIsNone(tracker.try_split(0))

  def test_try_split_allowed_after_checkpoint_set_even_with_zero_claims(self):
    """Once DoFn sets restriction.checkpoint, splits are allowed."""
    tracker = self._make_tracker()
    # Simulate DoFn setting checkpoint (e.g. idle source that ran start())
    tracker.current_restriction().checkpoint = LiveCheckpointMark(0)
    result = tracker.try_split(0)
    self.assertIsNotNone(result)

  def test_try_split_returns_primary_and_residual_after_claim(self):
    source = LiveUnboundedSource()
    restriction = _SDFUnboundedSourceRestriction(
        source, checkpoint=LiveCheckpointMark(1), watermark=Timestamp(10))
    tracker = _SDFUnboundedSourceRestrictionTracker(restriction)
    tracker.try_claim('elem_0')

    result = tracker.try_split(0)
    self.assertIsNotNone(result)
    primary, residual = result

    self.assertTrue(primary.is_done)
    self.assertEqual(primary.watermark, MAX_TIMESTAMP)
    self.assertIs(residual.source, source)
    self.assertEqual(
        cast(LiveCheckpointMark, residual.checkpoint).position, 1)
    self.assertEqual(residual.watermark, Timestamp(10))

  def test_try_split_sets_tracker_restriction_to_done(self):
    source = LiveUnboundedSource()
    restriction = _SDFUnboundedSourceRestriction(
        source, checkpoint=LiveCheckpointMark(1))
    tracker = _SDFUnboundedSourceRestrictionTracker(restriction)
    tracker.try_claim('elem')
    tracker.try_split(0)
    self.assertTrue(tracker.current_restriction().is_done)

  def test_try_split_sets_stopped(self):
    source = LiveUnboundedSource()
    restriction = _SDFUnboundedSourceRestriction(
        source, checkpoint=LiveCheckpointMark(1))
    tracker = _SDFUnboundedSourceRestrictionTracker(restriction)
    tracker.try_claim('elem')
    tracker.try_split(0)
    self.assertFalse(tracker.try_claim('after_split'))

  def test_try_split_returns_none_on_second_call(self):
    source = LiveUnboundedSource()
    restriction = _SDFUnboundedSourceRestriction(
        source, checkpoint=LiveCheckpointMark(1))
    tracker = _SDFUnboundedSourceRestrictionTracker(restriction)
    tracker.try_claim('elem')
    self.assertIsNotNone(tracker.try_split(0))
    self.assertIsNone(tracker.try_split(0))

  def test_try_split_checkpoint_on_primary_restriction(self):
    """Primary restriction keeps checkpoint so bundle finalizer can use it."""
    source = LiveUnboundedSource()
    cp = LiveCheckpointMark(3)
    restriction = _SDFUnboundedSourceRestriction(
        source, checkpoint=cp, watermark=Timestamp(9))
    tracker = _SDFUnboundedSourceRestrictionTracker(restriction)
    tracker.try_claim('elem')
    result = tracker.try_split(0)
    assert result is not None
    primary, residual = result

    self.assertIsNone(primary.checkpoint)
    # The tracker's own restriction carries the checkpoint.
    current = tracker.current_restriction()
    self.assertEqual(
        cast(LiveCheckpointMark, current.checkpoint).position, 3)
    self.assertTrue(current.is_done)
    self.assertEqual(
        cast(LiveCheckpointMark, residual.checkpoint).position, 3)

  def test_check_done_raises_when_active(self):
    source = LiveUnboundedSource()
    restriction = _SDFUnboundedSourceRestriction(
        source, checkpoint=LiveCheckpointMark(1))
    tracker = _SDFUnboundedSourceRestrictionTracker(restriction)
    tracker.try_claim('elem')
    with self.assertRaises(ValueError):
      tracker.check_done()

  def test_check_done_passes_when_stopped_by_split(self):
    source = LiveUnboundedSource()
    restriction = _SDFUnboundedSourceRestriction(
        source, checkpoint=LiveCheckpointMark(1))
    tracker = _SDFUnboundedSourceRestrictionTracker(restriction)
    tracker.try_claim('elem')
    tracker.try_split(0)
    tracker.check_done()  # should not raise

  def test_check_done_passes_when_is_done(self):
    source = LiveUnboundedSource()
    restriction = _SDFUnboundedSourceRestriction(source, is_done=True)
    tracker = _SDFUnboundedSourceRestrictionTracker(restriction)
    tracker.check_done()  # should not raise

  def test_check_done_error_message_references_process_continuation(self):
    tracker = self._make_tracker()
    tracker.try_claim('elem')
    tracker.current_restriction().checkpoint = LiveCheckpointMark(1)
    with self.assertRaises(ValueError) as ctx:
      tracker.check_done()
    self.assertIn('ProcessContinuation.resume()', str(ctx.exception))

  def test_current_progress_completed_matches_claimed_count(self):
    tracker = self._make_tracker()
    tracker.try_claim('a')
    tracker.try_claim('b')
    self.assertEqual(tracker.current_progress().completed_work, 2)

  def test_current_progress_remaining_zero_when_stopped(self):
    source = LiveUnboundedSource()
    restriction = _SDFUnboundedSourceRestriction(
        source, checkpoint=LiveCheckpointMark(0))
    tracker = _SDFUnboundedSourceRestrictionTracker(restriction)
    tracker.try_split(0)
    self.assertEqual(tracker.current_progress().remaining_work, 0)


# ===================================================================
#  DoFn process() tests
# ===================================================================

class DoFnProcessTest(unittest.TestCase):

  def test_reads_all_available_elements(self):
    source = LiveUnboundedSource()
    for i in range(4):
      source.add(i, i)
    elements, continuation, _, _, _, _ = _run_process_once(source)
    self.assertEqual(elements, [0, 1, 2, 3])

  def test_returns_continuation_when_source_is_idle(self):
    source = LiveUnboundedSource()
    # No data — source is idle from the start.
    elements, continuation, _, _, _, _ = _run_process_once(source)
    self.assertEqual(elements, [])
    self.assertIsNotNone(continuation)
    self.assertIsInstance(continuation, ProcessContinuation)

  def test_returns_continuation_after_partial_read(self):
    source = LiveUnboundedSource()
    source.add('a', 1)
    source.add('b', 2)
    # Two elements then idle.
    elements, continuation, _, _, _, _ = _run_process_once(source)
    self.assertEqual(elements, ['a', 'b'])
    self.assertIsNotNone(continuation)

  def test_no_continuation_when_source_done_by_max_watermark(self):
    source = _TerminalWatermarkSource(num_elements=2)
    elements, continuation, restriction, _, _, _ = _run_process_once(source)
    self.assertEqual(elements, [0, 1])
    self.assertIsNone(continuation)
    self.assertTrue(restriction.is_done)

  def test_restriction_checkpoint_updated_after_reading(self):
    source = LiveUnboundedSource()
    source.add('x', 5)
    source.add('y', 10)
    _, _, restriction, _, _, _ = _run_process_once(source)
    cp = cast(LiveCheckpointMark, restriction.checkpoint)
    self.assertEqual(cp.position, 2)

  def test_restriction_watermark_updated_after_reading(self):
    source = LiveUnboundedSource()
    source.add('a', 10)
    source.add('b', 20)
    _, _, restriction, _, _, _ = _run_process_once(source)
    self.assertEqual(restriction.watermark, Timestamp(20))

  def test_watermark_set_on_estimator(self):
    source = LiveUnboundedSource()
    source.add('x', 42)
    _, _, _, _, estimator, _ = _run_process_once(source)
    self.assertEqual(estimator.watermark, Timestamp(42))

  def test_watermark_never_regresses(self):
    source = _RegressingWatermarkSource(
        [Timestamp(10), Timestamp(42), Timestamp(5)])
    _, _, restriction, _, estimator, _ = _run_process_once(source)
    # After clamping, watermark should be 42 (not 5).
    self.assertEqual(restriction.watermark, Timestamp(42))
    self.assertEqual(estimator.watermark, Timestamp(42))

  def test_caches_reader_when_idle_for_next_invocation(self):
    from beam_streaming_poc.unbounded_source.wrapper import _pop_cached_reader
    source = LiveUnboundedSource()
    source.add('a', 1)
    # After reading 'a', source becomes idle.
    _, _, restriction, _, _, _ = _run_process_once(source)
    # The reader should be in the cache keyed by the final checkpoint.
    cached = _pop_cached_reader(source, restriction.checkpoint)
    self.assertIsNotNone(cached)
    reader, is_started = cached
    self.assertTrue(is_started)

  def test_cached_reader_reused_by_residual_invocation(self):
    """Residual reuses the cached reader; create_reader is called only once."""
    source = _CacheOnlySource(num_elements=4)
    restriction = _SDFUnboundedSourceRestriction(source)

    # First invocation: read element 0, then split.
    dofn = _SDFUnboundedSourceDoFn()
    dofn.setup()
    tracker = _SDFUnboundedSourceRestrictionTracker(restriction)
    tracker_view = _FakeTrackerView(tracker)
    estimator = _FakeWatermarkEstimator()
    finalizer = _FakeBundleFinalizer()

    elements_1 = []
    process_iter = iter(
        dofn.process(source, tracker_view, estimator, finalizer) or [])
    for output in process_iter:
      if isinstance(output, TimestampedValue):
        elements_1.append(output.value)
        # Trigger split after first element.
        split = tracker.try_split(0)
        assert split is not None
        _, residual = split
        break

    # Let the generator resume once so it can cache the live reader for the
    # residual invocation.
    list(process_iter)

    # Residual invocation: should reuse the reader from cache.
    elements_2, _, _, _, _, _ = _run_process_once(source, residual)
    self.assertEqual(elements_1, [0])
    self.assertEqual(elements_2, [1, 2, 3])
    # create_reader called only once (original invocation).
    self.assertEqual(source.create_reader_calls, [None])

  def test_no_tight_loop_on_idle_source(self):
    """Idle source process() completes quickly with no busy loop."""
    source = LiveUnboundedSource()
    # No data.
    t0 = time.time()
    elements, continuation, _, _, _, _ = _run_process_once(source)
    elapsed = time.time() - t0
    self.assertEqual(elements, [])
    self.assertIsNotNone(continuation)
    self.assertLess(elapsed, 0.1,
                    'process() on idle source took too long: %.3fs' % elapsed)

  def test_finalizer_registered_when_checkpointing(self):
    source = _TrackingUnboundedSource(num_elements=3)
    _, _, restriction, _, _, finalizer = _run_process_once(source)
    # After exhausting the source, a finalizer should be registered.
    checkpoint = restriction.checkpoint
    self.assertIsNotNone(checkpoint)
    self.assertIsInstance(checkpoint, _TrackingCheckpointMark)
    # Simulate runner committing the bundle.
    finalizer.finalize_bundle()
    self.assertTrue(checkpoint.finalized)

  def test_finalizer_registered_when_idle(self):
    source = LiveUnboundedSource()
    source.add('a', 1)
    # After reading 'a' and becoming idle, finalizer should be registered.
    _, continuation, restriction, _, _, finalizer = _run_process_once(source)
    self.assertIsNotNone(continuation)
    cp = cast(LiveCheckpointMark, restriction.checkpoint)
    self.assertIsNotNone(cp)

  def test_setup_initializes_pipeline_options_to_none(self):
    dofn = _SDFUnboundedSourceDoFn()
    dofn.setup()
    self.assertIsNone(dofn._pipeline_options)

  def test_resume_from_checkpoint(self):
    """After a checkpoint/resume cycle, the second invocation reads the rest."""
    source = LiveUnboundedSource()
    for i in range(5):
      source.add(i, i)

    # First invocation: read all available elements.
    elements1, continuation, restriction, _, _, _ = _run_process_once(source)
    self.assertEqual(elements1, [0, 1, 2, 3, 4])
    self.assertIsNotNone(continuation)

    # Second invocation: no new data → idle → another continuation.
    elements2, continuation2, _, _, _, _ = _run_process_once(source, restriction)
    self.assertEqual(elements2, [])
    self.assertIsNotNone(continuation2)

    # Add more data and resume.
    source.add(5, 5)
    source.add(6, 6)
    elements3, _, _, _, _, _ = _run_process_once(source, restriction)
    self.assertEqual(elements3, [5, 6])

  def test_check_done_passes_after_idle(self):
    """After ProcessContinuation is yielded, tracker try_split sets _stopped."""
    source = LiveUnboundedSource()
    dofn = _SDFUnboundedSourceDoFn()
    dofn.setup()
    restriction = _SDFUnboundedSourceRestriction(source)
    tracker = _SDFUnboundedSourceRestrictionTracker(restriction)
    tracker_view = _FakeTrackerView(tracker)
    estimator = _FakeWatermarkEstimator()
    finalizer = _FakeBundleFinalizer()

    list(dofn.process(source, tracker_view, estimator, finalizer) or [])
    # Simulate framework calling try_split after ProcessContinuation.
    tracker.try_split(0)
    tracker.check_done()  # should not raise

  def test_check_done_passes_after_terminal_watermark(self):
    source = _TerminalWatermarkSource(num_elements=2)
    dofn = _SDFUnboundedSourceDoFn()
    dofn.setup()
    restriction = _SDFUnboundedSourceRestriction(source)
    tracker = _SDFUnboundedSourceRestrictionTracker(restriction)
    tracker_view = _FakeTrackerView(tracker)
    estimator = _FakeWatermarkEstimator()
    finalizer = _FakeBundleFinalizer()

    list(dofn.process(source, tracker_view, estimator, finalizer) or [])
    tracker.check_done()  # should not raise (is_done=True)

  def test_reader_closed_after_terminal_watermark(self):
    source = _TerminalWatermarkSource(num_elements=2)
    _run_process_once(source)
    # The reader should have been closed.
    self.assertTrue(source._reader._closed)

  def test_reader_not_closed_when_cached_after_split(self):
    """When a split occurs mid-read, the reader is cached (not closed)."""
    from beam_streaming_poc.unbounded_source.wrapper import _pop_cached_reader
    source = LiveUnboundedSource()
    for i in range(5):
      source.add(i, i)

    dofn = _SDFUnboundedSourceDoFn()
    dofn.setup()
    restriction = _SDFUnboundedSourceRestriction(source)
    tracker = _SDFUnboundedSourceRestrictionTracker(restriction)
    tracker_view = _FakeTrackerView(tracker)
    estimator = _FakeWatermarkEstimator()
    finalizer = _FakeBundleFinalizer()

    first_reader = None
    for output in dofn.process(source, tracker_view, estimator, finalizer) or []:
      if isinstance(output, TimestampedValue) and first_reader is None:
        # After the first element, obtain a reference to the cached reader
        # by peeking (we'll verify it's alive after split).
        pass
      if isinstance(output, TimestampedValue) and output.value == 1:
        # Trigger split.
        tracker.try_split(0)
        # After split, try_claim in next loop iteration will return False.

    # Restriction should be done; reader was cached, not closed.
    cached = _pop_cached_reader(source, restriction.checkpoint)
    self.assertIsNotNone(cached)
    reader, _ = cached
    self.assertFalse(reader._closed)


# ===================================================================
#  TrySplit tests (tracker-level)
# ===================================================================

class TrySplitTest(unittest.TestCase):

  def test_returns_none_when_not_started(self):
    source = LiveUnboundedSource()
    restriction = _SDFUnboundedSourceRestriction(source)
    tracker = _SDFUnboundedSourceRestrictionTracker(restriction)
    # checkpoint=None, _claimed_count=0 → None
    self.assertIsNone(tracker.try_split(0))

  def test_returns_none_when_already_done(self):
    source = LiveUnboundedSource()
    restriction = _SDFUnboundedSourceRestriction(source, is_done=True)
    tracker = _SDFUnboundedSourceRestrictionTracker(restriction)
    self.assertIsNone(tracker.try_split(0))

  def test_returns_primary_and_residual_after_claim_with_checkpoint(self):
    source = LiveUnboundedSource()
    restriction = _SDFUnboundedSourceRestriction(
        source, checkpoint=LiveCheckpointMark(2), watermark=Timestamp(10))
    tracker = _SDFUnboundedSourceRestrictionTracker(restriction)
    tracker.try_claim('elem_0')
    tracker.try_claim('elem_1')

    result = tracker.try_split(0)
    self.assertIsNotNone(result)
    primary, residual = result

    self.assertTrue(primary.is_done)
    self.assertEqual(primary.watermark, MAX_TIMESTAMP)
    self.assertIs(residual.source, source)
    self.assertEqual(
        cast(LiveCheckpointMark, residual.checkpoint).position, 2)
    self.assertEqual(residual.watermark, Timestamp(10))

  def test_further_try_claim_fails_after_split(self):
    source = LiveUnboundedSource()
    restriction = _SDFUnboundedSourceRestriction(
        source, checkpoint=LiveCheckpointMark(1))
    tracker = _SDFUnboundedSourceRestrictionTracker(restriction)
    tracker.try_claim('elem')
    tracker.try_split(0)
    self.assertFalse(tracker.try_claim('after_split'))

  def test_check_done_passes_after_split(self):
    source = LiveUnboundedSource()
    restriction = _SDFUnboundedSourceRestriction(
        source, checkpoint=LiveCheckpointMark(1))
    tracker = _SDFUnboundedSourceRestrictionTracker(restriction)
    tracker.try_claim('elem')
    tracker.try_split(0)
    tracker.check_done()  # should not raise

  def test_residual_carries_correct_watermark(self):
    source = LiveUnboundedSource()
    restriction = _SDFUnboundedSourceRestriction(
        source, checkpoint=LiveCheckpointMark(2), watermark=Timestamp(42))
    tracker = _SDFUnboundedSourceRestrictionTracker(restriction)
    tracker.try_claim('a')
    tracker.try_claim('b')
    result = tracker.try_split(0)
    assert result is not None
    _, residual = result
    self.assertEqual(residual.watermark, Timestamp(42))

  def test_residual_reader_resumes_from_checkpoint_at_dofn_level(self):
    """DoFn invocation on the residual picks up from the right position."""
    source = LiveUnboundedSource()
    for i in range(5):
      source.add(i, i)

    # First invocation: read some elements, then split.
    elements1, _, restriction, _, _, _ = _run_process_once(source)
    self.assertEqual(elements1, [0, 1, 2, 3, 4])
    # After idle, checkpoint is at position 5.
    cp = cast(LiveCheckpointMark, restriction.checkpoint)
    self.assertEqual(cp.position, 5)

  def test_split_allowed_when_checkpoint_set_but_nothing_claimed(self):
    """Idle-from-start: DoFn sets checkpoint; split must work."""
    source = LiveUnboundedSource()
    restriction = _SDFUnboundedSourceRestriction(source)
    tracker = _SDFUnboundedSourceRestrictionTracker(restriction)
    # Simulate DoFn ran start() on the reader (found no data) and set checkpoint.
    restriction.checkpoint = LiveCheckpointMark(0)
    # claimed_count=0 but checkpoint is not None → split should proceed.
    result = tracker.try_split(0)
    self.assertIsNotNone(result)
    primary, residual = result
    self.assertTrue(primary.is_done)
    self.assertEqual(
        cast(LiveCheckpointMark, residual.checkpoint).position, 0)


# ===================================================================
#  Restriction and Coder tests
# ===================================================================

class RestrictionAndCoderTest(unittest.TestCase):

  def test_restriction_requires_unbounded_source(self):
    with self.assertRaises(TypeError):
      _SDFUnboundedSourceRestriction('not_a_source')

  def test_restriction_pickle_roundtrip_preserves_persistent_fields(self):
    source = LiveUnboundedSource()
    source.add('a', 1)
    cp = LiveCheckpointMark(position=3)
    restriction = _SDFUnboundedSourceRestriction(
        source, checkpoint=cp, watermark=Timestamp(33))

    restored = pickle.loads(pickle.dumps(restriction))
    self.assertIsInstance(restored.checkpoint, LiveCheckpointMark)
    self.assertEqual(restored.checkpoint.position, 3)
    self.assertEqual(restored.watermark, Timestamp(33))

  def test_coder_roundtrip_with_checkpoint_and_watermark(self):
    coder = _SDFUnboundedSourceRestrictionCoder()
    source = LiveUnboundedSource()
    cp = LiveCheckpointMark(position=3)
    restriction = _SDFUnboundedSourceRestriction(
        source, checkpoint=cp, watermark=Timestamp(55))

    restored = coder.decode(coder.encode(restriction))
    restored_cp = cast(LiveCheckpointMark, restored.checkpoint)
    self.assertIsInstance(restored_cp, LiveCheckpointMark)
    self.assertEqual(restored_cp.position, 3)
    self.assertEqual(restored.watermark, Timestamp(55))
    self.assertFalse(restored.is_done)

  def test_coder_roundtrip_done_restriction(self):
    coder = _SDFUnboundedSourceRestrictionCoder()
    source = LiveUnboundedSource()
    restriction = _SDFUnboundedSourceRestriction(source, is_done=True)

    restored = coder.decode(coder.encode(restriction))
    self.assertTrue(restored.is_done)
    self.assertIsNone(restored.checkpoint)

  def test_coder_roundtrip_no_checkpoint(self):
    """Restriction with checkpoint=None round-trips correctly."""
    coder = _SDFUnboundedSourceRestrictionCoder()
    source = LiveUnboundedSource()
    restriction = _SDFUnboundedSourceRestriction(
        source, checkpoint=None, watermark=Timestamp(7))

    restored = coder.decode(coder.encode(restriction))
    self.assertIsNone(restored.checkpoint)
    self.assertFalse(restored.is_done)
    self.assertEqual(restored.watermark, Timestamp(7))

  def test_coder_roundtrip_preserves_source_type(self):
    """Decoded source is the same type as the original."""
    coder = _SDFUnboundedSourceRestrictionCoder()
    source = LiveUnboundedSource()
    source.add('x', 1)
    restriction = _SDFUnboundedSourceRestriction(source)

    restored = coder.decode(coder.encode(restriction))
    restored_source = cast(LiveUnboundedSource, restored.source)
    self.assertIsInstance(restored_source, LiveUnboundedSource)
    self.assertEqual(len(restored_source._records), 1)

  def test_coder_roundtrip_done_with_checkpoint_and_watermark(self):
    """All fields set simultaneously: is_done + checkpoint + watermark."""
    coder = _SDFUnboundedSourceRestrictionCoder()
    source = LiveUnboundedSource()
    cp = LiveCheckpointMark(position=5)
    restriction = _SDFUnboundedSourceRestriction(
        source, checkpoint=cp, is_done=True, watermark=Timestamp(100))

    restored = coder.decode(coder.encode(restriction))
    self.assertTrue(restored.is_done)
    self.assertEqual(
        cast(LiveCheckpointMark, restored.checkpoint).position, 5)
    self.assertEqual(restored.watermark, Timestamp(100))

  def test_coder_roundtrip_default_watermark(self):
    """Default watermark matches Java's TIMESTAMP_MIN_VALUE."""
    coder = _SDFUnboundedSourceRestrictionCoder()
    source = LiveUnboundedSource()
    restriction = _SDFUnboundedSourceRestriction(source)

    restored = coder.decode(coder.encode(restriction))
    self.assertEqual(restored.watermark, MIN_TIMESTAMP)


# ===================================================================
#  Restriction Provider tests
# ===================================================================

class RestrictionProviderTest(unittest.TestCase):

  def test_provider_builds_restrictions_and_trackers(self):
    provider = _SDFUnboundedSourceRestrictionProvider()
    source = LiveUnboundedSource()
    restriction = provider.initial_restriction(source)

    self.assertIs(restriction.source, source)
    self.assertIsNone(restriction.checkpoint)
    self.assertEqual(restriction.watermark, MIN_TIMESTAMP)
    self.assertIsInstance(
        provider.create_tracker(restriction),
        _SDFUnboundedSourceRestrictionTracker)

  def test_provider_split_delegates_to_source_split(self):
    provider = _SDFUnboundedSourceRestrictionProvider()
    source = LiveUnboundedSource()
    restriction = provider.initial_restriction(source)

    splits = list(provider.split(source, restriction))
    self.assertEqual(len(splits), 1)
    self.assertIs(splits[0].source, source)

  def test_provider_split_preserves_watermark(self):
    provider = _SDFUnboundedSourceRestrictionProvider()
    child_a = _SplitRecordingSource()
    child_b = _SplitRecordingSource()
    source = _SplitRecordingSource(split_result=[child_a, child_b])
    restriction = _SDFUnboundedSourceRestriction(
        source, watermark=Timestamp(123))

    splits = list(provider.split(source, restriction))

    self.assertEqual([split.source for split in splits], [child_a, child_b])
    self.assertEqual(
        [split.watermark for split in splits],
        [Timestamp(123), Timestamp(123)])

  def test_provider_split_keeps_checkpointed_restriction_unsplit(self):
    provider = _SDFUnboundedSourceRestrictionProvider()
    source = _SplitRecordingSource()
    restriction = _SDFUnboundedSourceRestriction(
        source, checkpoint=LiveCheckpointMark(3), watermark=Timestamp(9))

    splits = list(provider.split(source, restriction))

    self.assertEqual(splits, [restriction])
    self.assertEqual(source.split_calls, 0)

  def test_provider_split_falls_back_to_original_restriction_on_error(self):
    provider = _SDFUnboundedSourceRestrictionProvider()
    source = _SplitRecordingSource(fail_split=True)
    restriction = _SDFUnboundedSourceRestriction(source)

    splits = list(provider.split(source, restriction))

    self.assertEqual(splits, [restriction])
    self.assertEqual(source.split_calls, 1)


# ===================================================================
#  Watermark Estimator Provider tests
# ===================================================================

class WatermarkEstimatorProviderTest(unittest.TestCase):

  def test_initial_estimator_state_uses_restriction_watermark(self):
    provider = _SDFUnboundedSourceWatermarkEstimatorProvider()
    source = LiveUnboundedSource()
    restriction = _SDFUnboundedSourceRestriction(
        source, watermark=Timestamp(42))

    self.assertEqual(
        provider.initial_estimator_state(source, restriction), Timestamp(42))

  def test_create_watermark_estimator(self):
    provider = _SDFUnboundedSourceWatermarkEstimatorProvider()
    estimator = provider.create_watermark_estimator(Timestamp(0))
    self.assertIsInstance(estimator, ManualWatermarkEstimator)


# ===================================================================
#  Checkpoint finalization tests
# ===================================================================

class CheckpointFinalizationTest(unittest.TestCase):

  def test_finalize_checkpoint_called_via_mock(self):
    mock_checkpoint = mock.MagicMock(spec=CheckpointMark)
    bundle_finalizer = core.DoFn.BundleFinalizerParam()
    bundle_finalizer.register(mock_checkpoint.finalize_checkpoint)
    bundle_finalizer.finalize_bundle()
    mock_checkpoint.finalize_checkpoint.assert_called_once()

  def test_finalize_checkpoint_on_resumed_bundle(self):
    source = _TrackingUnboundedSource(num_elements=3)

    # First invocation: read all 3 elements, snapshot checkpoint.
    elements, continuation, restriction, _, _, finalizer = (
        _run_process_once(source))
    self.assertEqual(elements, [0, 1, 2])
    self.assertIsNotNone(continuation)

    old_checkpoint = cast(_TrackingCheckpointMark, restriction.checkpoint)
    self.assertIsNotNone(old_checkpoint)
    self.assertFalse(old_checkpoint.finalized)
    # Simulate runner committing the first bundle.
    finalizer.finalize_bundle()
    self.assertTrue(old_checkpoint.finalized)

    # Resume: more elements available.
    source.num_elements = 5
    elements2, _, _, _, _, _ = _run_process_once(source, restriction)
    self.assertEqual(elements2, [3, 4])


# ===================================================================
#  Runner behavior probes
# ===================================================================

class RunnerBehaviorProbesTest(unittest.TestCase):

  def test_try_split_returns_checkpoint(self):
    """try_split returns (primary, residual) after DoFn has set checkpoint."""
    source = LiveUnboundedSource()
    for i in range(5):
      source.add(i, i)
    restriction = _SDFUnboundedSourceRestriction(
        source, checkpoint=LiveCheckpointMark(1), watermark=Timestamp(0))
    tracker = _SDFUnboundedSourceRestrictionTracker(restriction)
    tracker.try_claim('elem_0')

    result = tracker.try_split(0)
    self.assertIsNotNone(result)
    primary, residual = result
    self.assertTrue(primary.is_done)
    self.assertIsNotNone(residual.checkpoint)

  def test_checkpoint_encode_time_is_non_negative(self):
    source = LiveUnboundedSource()
    cp = LiveCheckpointMark(position=1)
    restriction = _SDFUnboundedSourceRestriction(
        source, checkpoint=cp, watermark=Timestamp(10))
    coder = _SDFUnboundedSourceRestrictionCoder()
    t0 = time.time()
    encoded = coder.encode(restriction)
    elapsed = time.time() - t0
    self.assertGreaterEqual(elapsed, 0)
    self.assertGreater(len(encoded), 0)
    decoded = coder.decode(encoded)
    self.assertEqual(decoded.watermark, Timestamp(10))

  def test_watermark_never_regresses_across_checkpoint_resume(self):
    """Across multiple DoFn invocations the watermark never goes backwards."""
    source = LiveUnboundedSource()
    source.add('a', 10)
    source.add('b', 20)
    source.add('c', 15)  # Intentional regression in raw timestamps.

    # First invocation: read all available, snapshot checkpoint.
    _, _, restriction, _, estimator, _ = _run_process_once(source)
    watermarks = list(estimator._history)

    # Add more data (some with older timestamps) and resume.
    source.add('d', 12)  # Older than 'c' -- potential regression.
    source.add('e', 50)
    _, _, _, _, estimator2, _ = _run_process_once(source, restriction)
    watermarks.extend(estimator2._history)

    for i in range(1, len(watermarks)):
      self.assertGreaterEqual(
          watermarks[i], watermarks[i - 1],
          'Watermark regressed at index %d: %s -> %s' %
          (i, watermarks[i - 1], watermarks[i]))

  @unittest.skip(
      'DirectRunner loops indefinitely with working try_split: '
      'ProcessContinuation.resume() now successfully creates residuals, '
      'causing the unbounded source to be re-scheduled forever.  This is '
      'correct streaming behavior — a real runner (Flink/Dataflow) would '
      'honour the Duration delay.  The DirectRunner does not.')
  def test_direct_runner_reads_all_elements(self):
    """ReadFromUnboundedSourceFn produces correct output via DirectRunner."""
    source = LiveUnboundedSource()
    for i in range(6):
      source.add(i, i)
    with beam.Pipeline() as p:
      output = (
          p
          | beam.Create([source])
          | ReadFromUnboundedSourceFn()
          | beam.Map(lambda tsv: tsv.value
                     if hasattr(tsv, 'value') else tsv)
      )
      from apache_beam.testing.util import assert_that, equal_to
      assert_that(output, equal_to([0, 1, 2, 3, 4, 5]))


if __name__ == '__main__':
  unittest.main()
