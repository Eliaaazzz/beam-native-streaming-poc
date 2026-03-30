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

"""WatchGrowthFn SDF and the public ``Watch`` API.

Usage::

    Watch.growth_of(poll_fn)
        .with_poll_interval(duration)
        .with_termination_per_input(condition)
        .with_output_key_fn(key_fn)
        .with_output_coder(coder)
        .with_output_key_coder(coder)
"""

from __future__ import annotations

import collections
import hashlib
import logging
import math
import time
from typing import Any
from typing import Callable
from typing import Optional
from typing import Protocol
from typing import Tuple
from typing import cast

import apache_beam as beam
from apache_beam import coders
from apache_beam import typehints
from apache_beam.internal import pickler
from apache_beam.io.watermark_estimators import ManualWatermarkEstimator
from apache_beam.runners.sdf_utils import RestrictionTrackerView
from apache_beam.transforms import core
from apache_beam.transforms.core import ProcessContinuation
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils.timestamp import MAX_TIMESTAMP
from apache_beam.utils.timestamp import Timestamp

from beam_streaming_poc.watch.poll import PollResult
from beam_streaming_poc.watch.growth_state import GrowthState
from beam_streaming_poc.watch.growth_state import NonPollingGrowthState
from beam_streaming_poc.watch.growth_state import PollingGrowthState
from beam_streaming_poc.watch.growth_state import _GrowthStateCoder
from beam_streaming_poc.watch.growth_state import _GrowthStateTracker
from beam_streaming_poc.watch.termination import TerminationCondition
from beam_streaming_poc.watch.termination import never
from beam_streaming_poc.watch.termination import _duration_to_secs
from beam_streaming_poc.watch.termination import _validate_poll_interval_secs

LOG = logging.getLogger(__name__)


class _SupportsSetWatermark(Protocol):
  def set_watermark(self, watermark: Timestamp) -> None:
    pass


def _identity(value):
  return value


def _validate_max_completed_size(
    max_completed_size: Optional[int]) -> Optional[int]:
  if max_completed_size is None:
    return None
  if isinstance(max_completed_size, bool) or not isinstance(
      max_completed_size, int):
    raise TypeError('max_completed_size must be an int or None')
  if max_completed_size < 0:
    raise ValueError('max_completed_size must be >= 0')
  return max_completed_size


def _validate_optional_coder(
    coder: Optional[coders.Coder],
    label: str) -> Optional[coders.Coder]:
  if coder is None:
    return None
  if not isinstance(coder, coders.Coder):
    raise TypeError('%s must be a Beam coder or None' % label)
  return coder


def _validate_deterministic_coder(
    coder: coders.Coder,
    label: str) -> coders.Coder:
  try:
    return coder.as_deterministic_coder(label)
  except ValueError as exc:
    raise ValueError('%s must be deterministic' % label) from exc


def _coder_type_hint(coder: Optional[coders.Coder]):
  if coder is None:
    return typehints.Any
  try:
    return coder.to_type_hint()
  except Exception:
    return typehints.Any


class WatchGrowthFn(
    beam.DoFn, core.RestrictionProvider, core.WatermarkEstimatorProvider):
  """Internal SDF powering Watch.growth_of(...)."""

  def __init__(
      self,
      poll_fn: Callable[[Any], Any],
      poll_interval_secs: float,
      termination_condition: TerminationCondition,
      output_key_fn: Callable[[Any], Any],
      max_completed_size: Optional[int],
      output_coder: Optional[coders.Coder] = None,
      output_key_coder: Optional[coders.Coder] = None):
    if not callable(poll_fn):
      raise TypeError('poll_fn must be callable')
    if not isinstance(termination_condition, TerminationCondition):
      raise TypeError('termination_condition must be a TerminationCondition')
    if not callable(output_key_fn):
      raise TypeError('output_key_fn must be callable')

    self._poll_fn = poll_fn
    self._poll_interval_secs = _validate_poll_interval_secs(poll_interval_secs)
    self._termination_condition = termination_condition
    self._output_key_fn = output_key_fn
    self._max_completed_size = _validate_max_completed_size(max_completed_size)
    self._output_coder = _validate_optional_coder(output_coder, 'output_coder')
    self._output_key_coder = _validate_optional_coder(
        output_key_coder, 'output_key_coder')
    if self._output_key_coder is None and output_key_fn is _identity:
      self._output_key_coder = self._output_coder
    if self._output_key_coder is not None:
      self._output_key_coder = _validate_deterministic_coder(
          self._output_key_coder, 'output_key_coder')
    self._restriction_coder = _GrowthStateCoder(output_coder=self._output_coder)

  # ---------- RestrictionProvider ----------

  def initial_restriction(self, element) -> GrowthState:
    now = Timestamp.of(time.time())
    return PollingGrowthState(
        completed=collections.OrderedDict(),
        poll_watermark=None,
        termination_state=self._termination_condition.for_new_input(
            now, element))

  def create_tracker(self, restriction: GrowthState):
    return _GrowthStateTracker(
        state=restriction,
        hash_fn=self._hash_fn,
        max_completed_size=self._max_completed_size)

  def split(self, element, restriction):
    yield restriction

  def restriction_size(self, element, restriction):
    if isinstance(restriction, NonPollingGrowthState):
      return max(1.0, float(len(restriction.pending_outputs)))
    return 1.0

  def restriction_coder(self):
    return self._restriction_coder

  def truncate(self, element, restriction):
    if isinstance(restriction, NonPollingGrowthState):
      return restriction
    return None

  # ---------- WatermarkEstimatorProvider ----------

  def initial_estimator_state(self, element, restriction: GrowthState):
    return element.timestamp if isinstance(element, TimestampedValue) else None

  def create_watermark_estimator(self, estimator_state):
    return ManualWatermarkEstimator(estimator_state)

  # ---------- SDF process ----------

  @beam.DoFn.unbounded_per_element()
  def process(
      self,
      element,
      restriction_tracker=beam.DoFn.RestrictionParam(),
      watermark_estimator=beam.DoFn.WatermarkEstimatorParam()):
    tracker = cast(RestrictionTrackerView, restriction_tracker)
    wm_estimator = cast(_SupportsSetWatermark, watermark_estimator)
    state = tracker.current_restriction()

    # ── Branch 1: NonPollingGrowthState (bounded replay) ──
    if isinstance(state, NonPollingGrowthState):
      claim_position = (
          state.pending_outputs, state.pending_watermark, None)
      if not tracker.try_claim(claim_position):
        return
      for discovered_ts, value in state.pending_outputs:
        yield TimestampedValue((element, value), discovered_ts)
      return

    # ── Branch 2: PollingGrowthState (active polling) ──
    polling_state = cast(PollingGrowthState, state)

    now = Timestamp.of(time.time())
    poll_result = self._normalize_poll_result(self._poll_fn(element))

    filtered_outputs = self._compute_never_seen_before(
        polling_state, poll_result, now)

    termination_state = polling_state.termination_state
    if filtered_outputs:
      termination_state = self._termination_condition.on_seen_new_output(
          Timestamp.of(time.time()), termination_state)
    termination_state = self._termination_condition.on_poll_complete(
        termination_state)

    watermark_candidate = poll_result.watermark
    if watermark_candidate is None and filtered_outputs:
      watermark_candidate = filtered_outputs[0][0]

    claim_position = (
        tuple(filtered_outputs), watermark_candidate, termination_state)
    if not tracker.try_claim(claim_position):
      return

    for discovered_ts, value in filtered_outputs:
      yield TimestampedValue((element, value), discovered_ts)

    if self._termination_condition.can_stop_polling(
        Timestamp.of(time.time()), termination_state):
      return

    if watermark_candidate == MAX_TIMESTAMP:
      return

    if watermark_candidate is not None:
      self._set_watermark_safe(watermark_candidate, wm_estimator)

    yield ProcessContinuation.resume(self._poll_interval_secs)
    return

  # ---------- internals ----------

  def _hash_fn(self, value: Any) -> bytes:
    """Composed hash: output_key_fn -> _stable_hash_128."""
    return self._stable_hash_128(
        self._output_key_fn(value), coder=self._output_key_coder)

  def _normalize_poll_result(self, value) -> PollResult:
    if isinstance(value, PollResult):
      return self._with_java_complete_semantics(value)
    if isinstance(value, tuple) and len(value) == 3:
      outputs, watermark, complete = value
      return self._with_java_complete_semantics(
          PollResult.of(outputs, watermark=watermark, complete=bool(complete)))
    return self._with_java_complete_semantics(PollResult.of(value))

  @staticmethod
  def _with_java_complete_semantics(poll_result: PollResult) -> PollResult:
    if poll_result.complete:
      return PollResult.of(
          poll_result.outputs, watermark=MAX_TIMESTAMP, complete=True)
    return poll_result

  def _compute_never_seen_before(
      self,
      state: PollingGrowthState,
      poll_result: PollResult,
      default_ts: Timestamp) -> list:
    seen_in_this_poll = collections.OrderedDict()

    for output in poll_result.outputs:
      discovered_ts, value = self._extract_output(output, default_ts)
      dedup_key = self._output_key_fn(value)
      dedup_hash = self._stable_hash_128(
          dedup_key, coder=self._output_key_coder)

      if dedup_hash in state.completed:
        continue
      if dedup_hash in seen_in_this_poll:
        continue

      seen_in_this_poll[dedup_hash] = (discovered_ts, value)

    result = sorted(seen_in_this_poll.values(), key=lambda pair: pair[0])
    return result

  @staticmethod
  def _extract_output(
      output: Any,
      default_discovery_ts: Timestamp) -> Tuple[Timestamp, Any]:
    if isinstance(output, TimestampedValue):
      return output.timestamp, output.value
    if (isinstance(output, tuple) and len(output) == 2 and
        isinstance(output[0], Timestamp)):
      return output[0], output[1]
    return default_discovery_ts, output

  @staticmethod
  def _set_watermark_safe(
      watermark: Optional[Timestamp],
      wm_estimator: _SupportsSetWatermark) -> None:
    if watermark is not None:
      try:
        wm_estimator.set_watermark(watermark)
      except (TypeError, ValueError):
        pass

  def _stable_hash_128(
      self,
      value: Any,
      coder: Optional[coders.Coder] = None) -> bytes:
    try:
      resolved_coder = coder or coders.registry.get_coder(type(value))
      if not resolved_coder.is_deterministic():
        raise ValueError('non-deterministic coder')
      payload = resolved_coder.encode(value)
    except Exception:
      LOG.warning(
          'No deterministic coder found for type %s; falling back to '
          'non-deterministic pickle for dedup hashing. This may cause '
          'incorrect deduplication across pipeline updates.',
          type(value).__name__)
      payload = pickler.dumps(value)

    return hashlib.blake2b(
        payload,
        digest_size=16,
        person=b'beam-watch-poc').digest()


# ===================================================================
#  PTransform wrapper and public API.
# ===================================================================

class _WatchGrowthTransform(beam.PTransform):
  """Builder-style transform for polling unbounded growth."""

  def __init__(
      self,
      poll_fn: Callable[[Any], Any],
      poll_interval_secs: float = 1.0,
      termination_condition: Optional[TerminationCondition] = None,
      output_key_fn: Optional[Callable[[Any], Any]] = None,
      max_completed_size: Optional[int] = None,
      output_coder: Optional[coders.Coder] = None,
      output_key_coder: Optional[coders.Coder] = None):
    super().__init__()
    if not callable(poll_fn):
      raise TypeError('poll_fn must be callable')
    if output_key_fn is not None and not callable(output_key_fn):
      raise TypeError('output_key_fn must be callable')

    self._poll_fn = poll_fn
    self._poll_interval_secs = _validate_poll_interval_secs(poll_interval_secs)
    self._termination_condition = termination_condition or never()
    self._output_key_fn = output_key_fn
    self._max_completed_size = _validate_max_completed_size(max_completed_size)
    self._output_coder = _validate_optional_coder(output_coder, 'output_coder')
    self._output_key_coder = _validate_optional_coder(
        output_key_coder, 'output_key_coder')

  def _copy(self, **overrides) -> '_WatchGrowthTransform':
    params = dict(
        poll_fn=self._poll_fn,
        poll_interval_secs=self._poll_interval_secs,
        termination_condition=self._termination_condition,
        output_key_fn=self._output_key_fn,
        max_completed_size=self._max_completed_size,
        output_coder=self._output_coder,
        output_key_coder=self._output_key_coder)
    params.update(overrides)
    return _WatchGrowthTransform(**params)

  def with_poll_interval(self, duration) -> '_WatchGrowthTransform':
    return self._copy(
        poll_interval_secs=_validate_poll_interval_secs(duration))

  def with_termination_per_input(
      self,
      condition: TerminationCondition) -> '_WatchGrowthTransform':
    if not isinstance(condition, TerminationCondition):
      raise TypeError('condition must be a TerminationCondition')
    return self._copy(termination_condition=condition)

  def with_output_key_fn(self, key_fn: Callable[[Any], Any]):
    if not callable(key_fn):
      raise TypeError('key_fn must be callable')
    return self._copy(output_key_fn=key_fn)

  def with_output_coder(self, output_coder: coders.Coder):
    return self._copy(
        output_coder=_validate_optional_coder(output_coder, 'output_coder'))

  def with_output_key_coder(self, output_key_coder: coders.Coder):
    return self._copy(
        output_key_coder=_validate_optional_coder(
            output_key_coder, 'output_key_coder'))

  def with_max_completed_size(self, max_completed_size: Optional[int]):
    return self._copy(
        max_completed_size=_validate_max_completed_size(max_completed_size))

  def expand(self, input_or_inputs):
    output_key_fn = self._output_key_fn or _identity
    effective_output_key_coder = self._output_key_coder
    if effective_output_key_coder is None and self._output_key_fn is None:
      effective_output_key_coder = self._output_coder

    if effective_output_key_coder is not None:
      _validate_deterministic_coder(
          effective_output_key_coder, 'output_key_coder')

    result = input_or_inputs | beam.ParDo(
        WatchGrowthFn(
            poll_fn=self._poll_fn,
            poll_interval_secs=self._poll_interval_secs,
            termination_condition=self._termination_condition,
            output_key_fn=output_key_fn,
            max_completed_size=self._max_completed_size,
            output_coder=self._output_coder,
            output_key_coder=effective_output_key_coder))

    try:
      input_type = input_or_inputs.element_type or typehints.Any
      output_type = _coder_type_hint(self._output_coder)
      result.element_type = typehints.KV[input_type, output_type]
    except Exception:
      result.element_type = typehints.KV[typehints.Any, typehints.Any]

    return result


class Watch:
  """User-facing Watch namespace for growth polling transforms."""

  @staticmethod
  def growth_of(
      poll_fn: Callable[[Any], Any],
      output_key_fn: Optional[Callable[[Any], Any]] = None
  ) -> _WatchGrowthTransform:
    """Create a growth polling transform from ``poll_fn``."""
    return _WatchGrowthTransform(poll_fn, output_key_fn=output_key_fn)
