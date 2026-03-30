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

from __future__ import annotations

import collections
import logging
import os
import subprocess
import sys
import unittest
from pathlib import Path
from typing import Any
from typing import cast

import apache_beam as beam
from apache_beam import coders
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.core import ProcessContinuation
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils.timestamp import MAX_TIMESTAMP
from apache_beam.utils.timestamp import Timestamp

from beam_streaming_poc.watch import after_iterations
from beam_streaming_poc.watch import after_time_since_new_output
from beam_streaming_poc.watch import after_total_of
from beam_streaming_poc.watch import DEFAULT_MAX_COMPLETED_SIZE
from beam_streaming_poc.watch import EMPTY_STATE
from beam_streaming_poc.watch import GrowthState
from beam_streaming_poc.watch import NonPollingGrowthState
from beam_streaming_poc.watch import PollingGrowthState
from beam_streaming_poc.watch import PollResult
from beam_streaming_poc.watch import Watch
from beam_streaming_poc.watch import WatchGrowthFn
from beam_streaming_poc.watch import _GrowthStateCoder
from beam_streaming_poc.watch import _GrowthStateTracker
from beam_streaming_poc.watch import never
from beam_streaming_poc.watch.growth_fn import _identity


def _make_fn(**kwargs):
  """Build a WatchGrowthFn with sensible defaults."""
  defaults = dict(
      poll_fn=lambda _: PollResult.empty(complete=True),
      poll_interval_secs=1.0,
      termination_condition=never(),
      output_key_fn=_identity,
      max_completed_size=None)
  defaults.update(kwargs)
  return WatchGrowthFn(**defaults)


def _make_tracker(state, fn=None, max_completed_size=None):
  """Build a _GrowthStateTracker for *state* using a real hash_fn."""
  if fn is None:
    fn = _make_fn(max_completed_size=max_completed_size)
  return _GrowthStateTracker(
      state=state,
      hash_fn=fn._hash_fn,
      max_completed_size=fn._max_completed_size)


class _NoopWatermarkEstimator:
  def set_watermark(self, watermark):
    del watermark


class _RecordingWatermarkEstimator:
  def __init__(self):
    self.calls = []

  def set_watermark(self, watermark):
    self.calls.append(watermark)


class _TrackerView:
  def __init__(self, tracker):
    self._tracker = tracker
    self.deferred = None

  def current_restriction(self):
    return self._tracker.current_restriction()

  def try_claim(self, position):
    return self._tracker.try_claim(position)

  def defer_remainder(self, deferred_time=None):
    self.deferred = deferred_time


class _Record:
  def __init__(self, value):
    self.value = value

  def __eq__(self, other):
    return isinstance(other, _Record) and self.value == other.value

  def __repr__(self):
    return '_Record(%r)' % self.value


class _RecordCoder(coders.Coder):
  def encode(self, value):
    return value.value.encode('utf-8')

  def decode(self, encoded):
    return _Record(encoded.decode('utf-8'))

  def is_deterministic(self):
    return True


class _Key:
  def __init__(self, value):
    self.value = value


class _KeyCoder(coders.Coder):
  def encode(self, value):
    return value.value.encode('utf-8')

  def decode(self, encoded):
    return _Key(encoded.decode('utf-8'))

  def is_deterministic(self):
    return True


# ===================================================================
#  PTransform / builder tests
# ===================================================================

class WatchBuilderTest(unittest.TestCase):
  def test_growth_of_rejects_non_callable_poll_fn(self):
    with self.assertRaises(TypeError):
      Watch.growth_of(cast(Any, 'not_callable'))

  def test_with_poll_interval_rejects_non_positive_values(self):
    transform = Watch.growth_of(lambda _: PollResult.empty())
    with self.assertRaises(ValueError):
      transform.with_poll_interval(0)
    with self.assertRaises(ValueError):
      transform.with_poll_interval(-1)

  def test_with_max_completed_size_rejects_negative(self):
    transform = Watch.growth_of(lambda _: PollResult.empty())
    with self.assertRaises(ValueError):
      transform.with_max_completed_size(-1)

  def test_with_output_key_fn_requires_callable(self):
    transform = Watch.growth_of(lambda _: PollResult.empty())
    with self.assertRaises(TypeError):
      transform.with_output_key_fn(cast(Any, 'not_callable'))

  def test_with_output_coder_requires_beam_coder(self):
    transform = Watch.growth_of(lambda _: PollResult.empty())
    with self.assertRaises(TypeError):
      transform.with_output_coder(cast(Any, 'not_a_coder'))

  def test_with_output_key_coder_requires_beam_coder(self):
    transform = Watch.growth_of(lambda _: PollResult.empty())
    with self.assertRaises(TypeError):
      transform.with_output_key_coder(cast(Any, 'not_a_coder'))

  def test_watch_growth_fn_rejects_invalid_arguments(self):
    with self.assertRaises(ValueError):
      WatchGrowthFn(
          poll_fn=lambda _: PollResult.empty(),
          poll_interval_secs=0,
          termination_condition=never(),
          output_key_fn=lambda x: x,
          max_completed_size=None)
    with self.assertRaises(ValueError):
      WatchGrowthFn(
          poll_fn=lambda _: PollResult.empty(),
          poll_interval_secs=1,
          termination_condition=never(),
          output_key_fn=lambda x: x,
          max_completed_size=-1)

  def test_builder_methods_return_new_transform(self):
    base = Watch.growth_of(lambda _: PollResult.empty())
    updated = base.with_poll_interval(10)
    self.assertIsNot(base, updated)
    self.assertEqual(base._poll_interval_secs, 1.0)
    self.assertEqual(updated._poll_interval_secs, 10.0)

  def test_builder_preserves_coders_immutably(self):
    base = Watch.growth_of(lambda _: PollResult.empty())
    output_coder = _RecordCoder()
    updated = base.with_output_coder(output_coder)
    self.assertIsNone(base._output_coder)
    self.assertIs(updated._output_coder, output_coder)


# ===================================================================
#  Tracker: try_claim validation
# ===================================================================

class TryClaimTest(unittest.TestCase):

  def test_polling_accepts_new_outputs(self):
    state = PollingGrowthState(completed=collections.OrderedDict())
    tracker = _make_tracker(state)
    position = (((Timestamp(seconds=1), 'a'),), None, None)
    self.assertTrue(tracker.try_claim(position))

  def test_polling_rejects_collision_with_completed(self):
    fn = _make_fn()
    h = fn._hash_fn('a')
    state = PollingGrowthState(
        completed=collections.OrderedDict(
            [(h, Timestamp(seconds=1))]))
    tracker = _make_tracker(state, fn=fn)
    position = (((Timestamp(seconds=2), 'a'),), None, None)
    self.assertFalse(tracker.try_claim(position))

  def test_nonpolling_accepts_exact_match(self):
    outputs = ((Timestamp(seconds=1), 'x'),)
    state = NonPollingGrowthState(pending_outputs=outputs)
    tracker = _make_tracker(state)
    position = (outputs, None, None)
    self.assertTrue(tracker.try_claim(position))

  def test_nonpolling_rejects_mismatch(self):
    outputs = ((Timestamp(seconds=1), 'x'),)
    state = NonPollingGrowthState(pending_outputs=outputs)
    tracker = _make_tracker(state)
    wrong = (((Timestamp(seconds=1), 'WRONG'),), None, None)
    self.assertFalse(tracker.try_claim(wrong))

  def test_should_stop_after_successful_claim(self):
    state = PollingGrowthState(completed=collections.OrderedDict())
    tracker = _make_tracker(state)
    position = (((Timestamp(seconds=1), 'a'),), None, None)
    self.assertTrue(tracker.try_claim(position))
    # Second claim rejected because should_stop is True.
    position2 = (((Timestamp(seconds=2), 'b'),), None, None)
    self.assertFalse(tracker.try_claim(position2))

  def test_empty_outputs_accepted_on_polling(self):
    state = PollingGrowthState(completed=collections.OrderedDict())
    tracker = _make_tracker(state)
    position = ((), None, None)
    self.assertTrue(tracker.try_claim(position))


# ===================================================================
#  Tracker: try_split three cases
# ===================================================================

class TrySplitTest(unittest.TestCase):

  def test_case1_nothing_claimed_primary_empty_residual_original(self):
    """Case 1: nothing claimed -> primary=EMPTY, residual=original."""
    state = PollingGrowthState(
        completed=collections.OrderedDict(),
        poll_watermark=Timestamp(seconds=10),
        termination_state='term')
    tracker = _make_tracker(state)
    result = tracker.try_split(0)
    self.assertIsNotNone(result)
    primary, residual = result
    self.assertIs(primary, EMPTY_STATE)
    self.assertIsInstance(residual, PollingGrowthState)
    self.assertEqual(residual.poll_watermark, Timestamp(seconds=10))

  def test_case2_claimed_nonpolling_residual_empty(self):
    """Case 2: claimed + NonPolling -> residual=EMPTY."""
    outputs = ((Timestamp(seconds=1), 'x'),)
    state = NonPollingGrowthState(pending_outputs=outputs)
    tracker = _make_tracker(state)
    tracker.try_claim((outputs, None, None))
    result = tracker.try_split(0)
    self.assertIsNotNone(result)
    primary, residual = result
    self.assertIsInstance(primary, NonPollingGrowthState)
    self.assertIs(residual, EMPTY_STATE)

  def test_case3_claimed_polling_primary_nonpolling_residual_polling(self):
    """Case 3: claimed + Polling -> primary=NonPolling, residual=Polling."""
    fn = _make_fn()
    state = PollingGrowthState(
        completed=collections.OrderedDict(),
        poll_watermark=Timestamp(seconds=5),
        termination_state='term')
    tracker = _make_tracker(state, fn=fn)
    outputs = (
        (Timestamp(seconds=10), 'a'),
        (Timestamp(seconds=20), 'b'))
    tracker.try_claim((outputs, Timestamp(seconds=20), 'new_term'))

    result = tracker.try_split(0)
    self.assertIsNotNone(result)
    primary, residual = result

    # Primary is NonPolling with the claimed outputs.
    self.assertIsInstance(primary, NonPollingGrowthState)
    self.assertEqual(len(primary.pending_outputs), 2)
    self.assertEqual(primary.pending_watermark, Timestamp(seconds=20))

    # Residual is Polling with merged completed hashes.
    self.assertIsInstance(residual, PollingGrowthState)
    self.assertEqual(len(residual.completed), 2)
    # Watermark = max(5, 20) = 20.
    self.assertEqual(residual.poll_watermark, Timestamp(seconds=20))
    self.assertEqual(residual.termination_state, 'new_term')

  def test_case3_merges_completed_hashes(self):
    """Case 3: residual's completed = original + claimed hashes."""
    fn = _make_fn()
    existing_hash = fn._hash_fn('existing')
    state = PollingGrowthState(
        completed=collections.OrderedDict(
            [(existing_hash, Timestamp(seconds=1))]),
        termination_state='t')
    tracker = _make_tracker(state, fn=fn)
    tracker.try_claim((
        ((Timestamp(seconds=2), 'new'),), Timestamp(seconds=2), 't'))
    _, residual = tracker.try_split(0)
    # Residual completed has both old and new hashes.
    new_hash = fn._hash_fn('new')
    self.assertIn(existing_hash, residual.completed)
    self.assertIn(new_hash, residual.completed)
    self.assertEqual(len(residual.completed), 2)

  def test_case3_watermark_is_max(self):
    """Case 3: residual watermark = max(original, claimed)."""
    state = PollingGrowthState(
        completed=collections.OrderedDict(),
        poll_watermark=Timestamp(seconds=100),
        termination_state=None)
    tracker = _make_tracker(state)
    tracker.try_claim(((), Timestamp(seconds=50), None))
    _, residual = tracker.try_split(0)
    self.assertEqual(residual.poll_watermark, Timestamp(seconds=100))

  def test_empty_state_returns_none(self):
    """EMPTY_STATE with no claim -> None (nothing to split)."""
    tracker = _make_tracker(EMPTY_STATE)
    self.assertIsNone(tracker.try_split(0))

  def test_should_stop_after_split(self):
    state = PollingGrowthState(completed=collections.OrderedDict())
    tracker = _make_tracker(state)
    tracker.try_split(0)
    # try_claim should fail after split.
    self.assertFalse(
        tracker.try_claim(((), None, None)))

  def test_case3_evicts_with_max_completed_size(self):
    """Case 3: max_completed_size eviction during merge."""
    fn = _make_fn(max_completed_size=2)
    h1 = fn._hash_fn('old1')
    h2 = fn._hash_fn('old2')
    state = PollingGrowthState(
        completed=collections.OrderedDict([
            (h1, Timestamp(seconds=1)),
            (h2, Timestamp(seconds=2))]),
        termination_state=None)
    tracker = _make_tracker(state, fn=fn, max_completed_size=2)
    tracker.try_claim((
        ((Timestamp(seconds=3), 'new'),), None, None))
    _, residual = tracker.try_split(0)
    # max_completed_size=2, but we had 2 + 1 = 3 -> evict oldest.
    self.assertEqual(len(residual.completed), 2)
    # Oldest (h1) should have been evicted.
    self.assertNotIn(h1, residual.completed)


# ===================================================================
#  Tracker: check_done and is_bounded
# ===================================================================

class TrackerMiscTest(unittest.TestCase):

  def test_check_done_raises_if_not_stopped(self):
    state = PollingGrowthState(completed=collections.OrderedDict())
    tracker = _make_tracker(state)
    with self.assertRaises(ValueError):
      tracker.check_done()

  def test_check_done_passes_after_claim(self):
    state = PollingGrowthState(completed=collections.OrderedDict())
    tracker = _make_tracker(state)
    tracker.try_claim(((), None, None))
    tracker.check_done()

  def test_check_done_passes_after_split(self):
    state = PollingGrowthState(completed=collections.OrderedDict())
    tracker = _make_tracker(state)
    tracker.try_split(0)
    tracker.check_done()

  def test_is_bounded_true_for_nonpolling(self):
    state = NonPollingGrowthState(pending_outputs=())
    tracker = _make_tracker(state)
    self.assertTrue(tracker.is_bounded())

  def test_is_bounded_true_for_empty_state(self):
    tracker = _make_tracker(EMPTY_STATE)
    self.assertTrue(tracker.is_bounded())

  def test_is_bounded_false_for_polling(self):
    state = PollingGrowthState(completed=collections.OrderedDict())
    tracker = _make_tracker(state)
    self.assertFalse(tracker.is_bounded())


# ===================================================================
#  Coder roundtrip
# ===================================================================

class GrowthStateCoderTest(unittest.TestCase):

  def test_roundtrip_empty_polling(self):
    coder = _GrowthStateCoder()
    state = PollingGrowthState(
        completed=collections.OrderedDict(),
        poll_watermark=None,
        termination_state=None)
    decoded = coder.decode(coder.encode(state))
    self.assertIsInstance(decoded, PollingGrowthState)
    self.assertEqual(len(decoded.completed), 0)
    self.assertIsNone(decoded.poll_watermark)

  def test_roundtrip_polling_with_completed(self):
    coder = _GrowthStateCoder()
    fn = _make_fn()
    h = fn._hash_fn('a')
    state = PollingGrowthState(
        completed=collections.OrderedDict([(h, Timestamp(seconds=42))]),
        poll_watermark=Timestamp(seconds=10),
        termination_state={'k': 'v'})
    decoded = coder.decode(coder.encode(state))
    self.assertIsInstance(decoded, PollingGrowthState)
    self.assertEqual(len(decoded.completed), 1)
    self.assertIn(h, decoded.completed)
    self.assertEqual(decoded.poll_watermark, Timestamp(seconds=10))
    self.assertEqual(decoded.termination_state, {'k': 'v'})

  def test_roundtrip_nonpolling_empty(self):
    coder = _GrowthStateCoder()
    state = NonPollingGrowthState(pending_outputs=(), pending_watermark=None)
    decoded = coder.decode(coder.encode(state))
    self.assertIsInstance(decoded, NonPollingGrowthState)
    self.assertEqual(len(decoded.pending_outputs), 0)
    self.assertIsNone(decoded.pending_watermark)

  def test_roundtrip_nonpolling_with_outputs(self):
    coder = _GrowthStateCoder()
    outputs = (
        (Timestamp(seconds=1), 'hello'),
        (Timestamp(seconds=2), 42))
    state = NonPollingGrowthState(
        pending_outputs=outputs,
        pending_watermark=Timestamp(seconds=5))
    decoded = coder.decode(coder.encode(state))
    self.assertIsInstance(decoded, NonPollingGrowthState)
    self.assertEqual(len(decoded.pending_outputs), 2)
    self.assertEqual(decoded.pending_outputs[0], (Timestamp(seconds=1), 'hello'))
    self.assertEqual(decoded.pending_outputs[1], (Timestamp(seconds=2), 42))
    self.assertEqual(decoded.pending_watermark, Timestamp(seconds=5))

  def test_roundtrip_empty_state_sentinel(self):
    coder = _GrowthStateCoder()
    decoded = coder.decode(coder.encode(EMPTY_STATE))
    self.assertIsInstance(decoded, NonPollingGrowthState)
    self.assertEqual(len(decoded.pending_outputs), 0)
    self.assertIsNone(decoded.pending_watermark)

  def test_roundtrip_nonpolling_with_explicit_output_coder(self):
    coder = _GrowthStateCoder(output_coder=_RecordCoder())
    state = NonPollingGrowthState(
        pending_outputs=((Timestamp(seconds=1), _Record('hello')),))
    decoded = coder.decode(coder.encode(state))
    self.assertEqual(
        decoded.pending_outputs, ((Timestamp(seconds=1), _Record('hello')),))


# ===================================================================
#  compute_never_seen_before
# ===================================================================

class ComputeNeverSeenBeforeTest(unittest.TestCase):

  def test_filters_completed(self):
    fn = _make_fn()
    h_a = fn._hash_fn('a')
    state = PollingGrowthState(
        completed=collections.OrderedDict(
            [(h_a, Timestamp(seconds=1))]))
    poll = PollResult.of(['a', 'b'])
    result = fn._compute_never_seen_before(
        state, poll, Timestamp(seconds=10))
    values = [v for _, v in result]
    self.assertEqual(values, ['b'])

  def test_deduplicates_within_poll(self):
    fn = _make_fn()
    state = PollingGrowthState(completed=collections.OrderedDict())
    poll = PollResult.of(['x', 'x', 'y'])
    result = fn._compute_never_seen_before(
        state, poll, Timestamp(seconds=1))
    values = [v for _, v in result]
    self.assertIn('x', values)
    self.assertIn('y', values)
    self.assertEqual(len(values), 2)

  def test_sorted_by_timestamp(self):
    fn = _make_fn()
    state = PollingGrowthState(completed=collections.OrderedDict())
    poll = PollResult.of([
        TimestampedValue('c', Timestamp(seconds=30)),
        TimestampedValue('a', Timestamp(seconds=10)),
        TimestampedValue('b', Timestamp(seconds=20))])
    result = fn._compute_never_seen_before(
        state, poll, Timestamp(seconds=1))
    timestamps = [ts for ts, _ in result]
    self.assertEqual(timestamps, [
        Timestamp(seconds=10),
        Timestamp(seconds=20),
        Timestamp(seconds=30)])

  def test_does_not_mutate_state(self):
    fn = _make_fn()
    state = PollingGrowthState(completed=collections.OrderedDict())
    poll = PollResult.of(['a', 'b'])
    fn._compute_never_seen_before(state, poll, Timestamp(seconds=1))
    # State completed should NOT have been modified.
    self.assertEqual(len(state.completed), 0)


# ===================================================================
#  process() branches
# ===================================================================

class ProcessTest(unittest.TestCase):

  def test_nonpolling_replays_outputs(self):
    """NonPollingGrowthState branch replays all pending outputs."""
    outputs = (
        (Timestamp(seconds=1), 'x'),
        (Timestamp(seconds=2), 'y'))
    state = NonPollingGrowthState(
        pending_outputs=outputs, pending_watermark=Timestamp(seconds=2))
    fn = _make_fn()
    tracker = _make_tracker(state, fn=fn)
    emitted = list(fn.process(
        'seed',
        restriction_tracker=tracker,
        watermark_estimator=_NoopWatermarkEstimator()))
    self.assertEqual(len(emitted), 2)
    self.assertEqual(emitted[0].value, ('seed', 'x'))
    self.assertEqual(emitted[1].value, ('seed', 'y'))

  def test_nonpolling_ignores_pending_watermark(self):
    outputs = ((Timestamp(seconds=1), 'x'),)
    state = NonPollingGrowthState(
        pending_outputs=outputs, pending_watermark=Timestamp(seconds=9))
    fn = _make_fn()
    tracker = _make_tracker(state, fn=fn)
    wm_estimator = _RecordingWatermarkEstimator()
    list(fn.process(
        'seed', restriction_tracker=tracker, watermark_estimator=wm_estimator))
    self.assertEqual(wm_estimator.calls, [])

  def test_nonpolling_empty_produces_nothing(self):
    """EMPTY_STATE produces no outputs."""
    fn = _make_fn()
    tracker = _make_tracker(EMPTY_STATE, fn=fn)
    emitted = list(fn.process(
        'seed',
        restriction_tracker=tracker,
        watermark_estimator=_NoopWatermarkEstimator()))
    self.assertEqual(len(emitted), 0)
    tracker.check_done()

  def test_polling_emits_new_outputs(self):
    """PollingGrowthState branch polls, filters, emits."""
    fn = _make_fn(
        poll_fn=lambda _: PollResult.of(['a', 'b'], complete=True))
    state = PollingGrowthState(completed=collections.OrderedDict())
    tracker = _make_tracker(state, fn=fn)
    emitted = list(fn.process(
        'seed',
        restriction_tracker=tracker,
        watermark_estimator=_NoopWatermarkEstimator()))
    values = [e.value for e in emitted]
    self.assertIn(('seed', 'a'), values)
    self.assertIn(('seed', 'b'), values)

  def test_polling_deduplicates_against_completed(self):
    """Outputs already in completed are not re-emitted."""
    fn = _make_fn(
        poll_fn=lambda _: PollResult.of(['a', 'b'], complete=True))
    h_a = fn._hash_fn('a')
    state = PollingGrowthState(
        completed=collections.OrderedDict(
            [(h_a, Timestamp(seconds=1))]))
    tracker = _make_tracker(state, fn=fn)
    emitted = list(fn.process(
        'seed',
        restriction_tracker=tracker,
        watermark_estimator=_NoopWatermarkEstimator()))
    values = [e.value for e in emitted]
    self.assertEqual(values, [('seed', 'b')])

  def test_polling_empty_incomplete_does_not_advance_watermark(self):
    fn = _make_fn(
        poll_fn=lambda _: PollResult.empty(complete=False))
    tracker = _TrackerView(_make_tracker(
        PollingGrowthState(completed=collections.OrderedDict()), fn=fn))
    wm_estimator = _RecordingWatermarkEstimator()
    emitted = list(fn.process(
        'seed',
        restriction_tracker=tracker,
        watermark_estimator=wm_estimator))
    # Empty poll emits only a ProcessContinuation (no data, no watermark set).
    continuations = [e for e in emitted if isinstance(e, ProcessContinuation)]
    data = [e for e in emitted if not isinstance(e, ProcessContinuation)]
    self.assertEqual(data, [])
    self.assertEqual(wm_estimator.calls, [])
    self.assertEqual(len(continuations), 1)

  def test_complete_poll_uses_max_timestamp_watermark_for_checkpoint(self):
    fn = _make_fn(
        poll_fn=lambda _: PollResult.of(['a'], complete=True))
    tracker = _make_tracker(
        PollingGrowthState(completed=collections.OrderedDict()), fn=fn)
    emitted = list(fn.process(
        'seed',
        restriction_tracker=tracker,
        watermark_estimator=_RecordingWatermarkEstimator()))
    self.assertEqual([e.value for e in emitted], [('seed', 'a')])
    primary, residual = tracker.try_split(0)
    self.assertIsInstance(primary, NonPollingGrowthState)
    self.assertEqual(primary.pending_watermark, MAX_TIMESTAMP)
    self.assertIsInstance(residual, PollingGrowthState)
    self.assertEqual(residual.poll_watermark, MAX_TIMESTAMP)


# ===================================================================
#  End-to-end pipeline test
# ===================================================================

class PipelineTest(unittest.TestCase):

  def test_watch_deduplicates_with_stable_keys(self):
    poll_fn = lambda _: PollResult.of([1, 1, 2], complete=True)
    options = PipelineOptions(flags=[], streaming=True)
    with TestPipeline(options=options) as p:
      outputs = (
          p
          | beam.Create(['seed'])
          | Watch.growth_of(poll_fn).with_poll_interval(0.01)
      )
      assert_that(outputs, equal_to([('seed', 1), ('seed', 2)]))


# ===================================================================
#  Hash stability
# ===================================================================

class StableHashTest(unittest.TestCase):

  def test_stable_hash_consistent_across_pythonhashseed(self):
    script = r'''
from beam_streaming_poc.watch import WatchGrowthFn, PollResult, never
fn = WatchGrowthFn(lambda _: PollResult.empty(complete=True), 1.0, never(), lambda x: x, None)
print(fn._stable_hash_128({'k': ['v', 1]}).hex())
'''

    module_root = Path(__file__).resolve().parent
    workspace_root = module_root.parent
    env1 = os.environ.copy()
    env2 = os.environ.copy()
    env1['PYTHONHASHSEED'] = '7'
    env2['PYTHONHASHSEED'] = '11'
    env1['PYTHONPATH'] = str(workspace_root) + os.pathsep + env1.get(
        'PYTHONPATH', '')
    env2['PYTHONPATH'] = str(workspace_root) + os.pathsep + env2.get(
        'PYTHONPATH', '')

    out1 = subprocess.check_output(
        [sys.executable, '-c', script], text=True, env=env1, cwd=workspace_root
    ).strip()
    out2 = subprocess.check_output(
        [sys.executable, '-c', script], text=True, env=env2, cwd=workspace_root
    ).strip()

    self.assertEqual(out1, out2)

  def test_warning_on_pickle_fallback(self):
    fn = _make_fn()

    class _UncoderableType:
      pass

    with self.assertLogs('beam_streaming_poc.watch.growth_fn',
                         level=logging.WARNING) as cm:
      fn._stable_hash_128(_UncoderableType())
    self.assertTrue(any('non-deterministic pickle' in msg for msg in cm.output))

  def test_explicit_output_coder_is_used_for_identity_hashing(self):
    fn = _make_fn(output_coder=_RecordCoder())
    with self.assertNoLogs('beam_streaming_poc.watch.growth_fn', level='WARNING'):
      first = fn._hash_fn(_Record('same'))
      second = fn._hash_fn(_Record('same'))
    self.assertEqual(first, second)

  def test_explicit_output_key_coder_is_used_for_custom_keys(self):
    fn = _make_fn(
        output_key_fn=lambda value: _Key(value['id']),
        output_key_coder=_KeyCoder())
    with self.assertNoLogs('beam_streaming_poc.watch.growth_fn', level='WARNING'):
      first = fn._hash_fn({'id': 'same'})
      second = fn._hash_fn({'id': 'same'})
    self.assertEqual(first, second)


# ===================================================================
#  initial_restriction / estimator
# ===================================================================

class RestrictionProviderTest(unittest.TestCase):

  def test_initial_restriction_is_polling(self):
    fn = _make_fn()
    restriction = fn.initial_restriction('seed')
    self.assertIsInstance(restriction, PollingGrowthState)
    self.assertEqual(len(restriction.completed), 0)
    self.assertIsNone(restriction.poll_watermark)

  def test_initial_estimator_state_from_element_timestamp(self):
    """Java seeds the watermark estimator from the element's event timestamp."""
    fn = _make_fn()
    element = TimestampedValue('seed', Timestamp(seconds=42))
    state = PollingGrowthState(
        completed=collections.OrderedDict(),
        poll_watermark=Timestamp(seconds=99))
    # Should use the element timestamp, not the restriction's poll_watermark.
    self.assertEqual(
        fn.initial_estimator_state(element, state), Timestamp(seconds=42))

  def test_initial_estimator_state_plain_element(self):
    fn = _make_fn()
    state = NonPollingGrowthState(
        pending_outputs=(),
        pending_watermark=Timestamp(seconds=7))
    self.assertIsNone(fn.initial_estimator_state('seed', state))


# ===================================================================
#  Termination condition parity
# ===================================================================

class TerminationConditionTest(unittest.TestCase):

  def test_after_total_of_uses_strictly_longer_than(self):
    condition = after_total_of(5)
    state = condition.for_new_input(Timestamp(seconds=0), None)
    state = condition.on_poll_complete(state)
    self.assertFalse(condition.can_stop_polling(Timestamp(seconds=5), state))
    state = condition.on_poll_complete(state)
    self.assertTrue(condition.can_stop_polling(Timestamp(seconds=6), state))

  def test_after_time_since_new_output_ignores_time_before_first_output(self):
    condition = after_time_since_new_output(5)
    state = condition.for_new_input(Timestamp(seconds=0), None)

    # No new output — timeOfLastNewOutput is None, can never stop.
    state = condition.on_poll_complete(state)
    self.assertFalse(
        condition.can_stop_polling(Timestamp(seconds=100), state))

    # New output seen at t=100.
    state = condition.on_seen_new_output(Timestamp(seconds=100), state)
    state = condition.on_poll_complete(state)
    self.assertFalse(
        condition.can_stop_polling(Timestamp(seconds=100), state))

    # At t=105, exactly at threshold — should not stop (strictly longer).
    state = condition.on_poll_complete(state)
    self.assertFalse(
        condition.can_stop_polling(Timestamp(seconds=105), state))

    # At t=106, past threshold — should stop.
    state = condition.on_poll_complete(state)
    self.assertTrue(
        condition.can_stop_polling(Timestamp(seconds=106), state))


# ===================================================================
#  after_iterations tests
# ===================================================================

class AfterIterationsTest(unittest.TestCase):

  def test_stops_after_n_polls(self):
    condition = after_iterations(3)
    state = condition.for_new_input(Timestamp(seconds=0), None)
    self.assertFalse(condition.can_stop_polling(Timestamp(seconds=0), state))
    state = condition.on_poll_complete(state)
    self.assertFalse(condition.can_stop_polling(Timestamp(seconds=0), state))
    state = condition.on_poll_complete(state)
    self.assertFalse(condition.can_stop_polling(Timestamp(seconds=0), state))
    state = condition.on_poll_complete(state)
    self.assertTrue(condition.can_stop_polling(Timestamp(seconds=0), state))

  def test_rejects_invalid_n(self):
    with self.assertRaises(ValueError):
      after_iterations(0)
    with self.assertRaises(ValueError):
      after_iterations(-1)

  def test_works_with_either_of(self):
    from beam_streaming_poc.watch import either_of
    condition = either_of(after_iterations(2), after_total_of(999))
    state = condition.for_new_input(Timestamp(seconds=0), None)
    state = condition.on_poll_complete(state)
    state = condition.on_poll_complete(state)
    self.assertTrue(condition.can_stop_polling(Timestamp(seconds=1), state))


# ===================================================================
#  Default max_completed_size test
# ===================================================================

class DefaultMaxCompletedSizeTest(unittest.TestCase):

  def test_default_is_bounded(self):
    transform = Watch.growth_of(lambda _: PollResult.empty(complete=True))
    self.assertEqual(transform._max_completed_size, DEFAULT_MAX_COMPLETED_SIZE)
    self.assertEqual(DEFAULT_MAX_COMPLETED_SIZE, 100_000)


if __name__ == '__main__':
  unittest.main()
