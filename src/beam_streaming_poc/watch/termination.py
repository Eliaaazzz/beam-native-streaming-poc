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

"""Composable TerminationCondition implementations for the Watch transform.

Conditions:
- ``never()``: Poll until ``PollResult`` reports completion.
- ``after_total_of(duration)``: Stop after fixed elapsed time.
- ``after_time_since_new_output(duration)``: Stop after a period with no new output.
- ``either_of(c1, c2)``: Boolean OR combinator.
- ``all_of(c1, c2)``: Boolean AND combinator.
"""

from __future__ import annotations

import abc
import math
from typing import Any

from apache_beam.utils.timestamp import Duration
from apache_beam.utils.timestamp import Timestamp


class TerminationCondition(abc.ABC):
  """Mirrors Java ``Watch.Growth.TerminationCondition``."""

  @abc.abstractmethod
  def for_new_input(self, now: Timestamp, input_element: Any) -> Any:
    """Create a new termination state for a newly arrived input."""
    raise NotImplementedError

  def on_seen_new_output(self, now: Timestamp, state: Any) -> Any:
    """Update state when the poll result contained new outputs."""
    return state

  def on_poll_complete(self, state: Any) -> Any:
    """Update state after every poll completion."""
    return state

  @abc.abstractmethod
  def can_stop_polling(self, now: Timestamp, state: Any) -> bool:
    """Return True if polling should stop for this input."""
    raise NotImplementedError


class _NeverTermination(TerminationCondition):
  def for_new_input(self, now: Timestamp, input_element: Any) -> Any:
    return 0

  def can_stop_polling(self, now, state):
    return False


class _AfterTotalOf(TerminationCondition):
  def __init__(self, duration_secs: float):
    self._duration_secs = float(duration_secs)

  def for_new_input(self, now: Timestamp, input_element: Any):
    return now

  def can_stop_polling(self, now, state):
    elapsed = (now - state).micros / 1_000_000.0
    return elapsed > self._duration_secs


class _AfterTimeSinceNewOutput(TerminationCondition):
  def __init__(self, duration_secs: float):
    self._duration_secs = float(duration_secs)

  def for_new_input(self, now: Timestamp, input_element: Any):
    return (None, self._duration_secs)

  def on_seen_new_output(self, now: Timestamp, state: Any) -> Any:
    return (now, state[1])

  def can_stop_polling(self, now, state):
    time_of_last_new_output, duration_secs = state
    if time_of_last_new_output is None:
      return False
    elapsed = (now - time_of_last_new_output).micros / 1_000_000.0
    return elapsed > duration_secs


class _EitherOf(TerminationCondition):
  def __init__(self, left: TerminationCondition, right: TerminationCondition):
    self._left = left
    self._right = right

  def for_new_input(self, now: Timestamp, input_element: Any):
    return (self._left.for_new_input(now, input_element),
            self._right.for_new_input(now, input_element))

  def on_seen_new_output(self, now, state):
    left_state, right_state = state
    return (self._left.on_seen_new_output(now, left_state),
            self._right.on_seen_new_output(now, right_state))

  def on_poll_complete(self, state):
    left_state, right_state = state
    return (self._left.on_poll_complete(left_state),
            self._right.on_poll_complete(right_state))

  def can_stop_polling(self, now, state):
    left_state, right_state = state
    return (self._left.can_stop_polling(now, left_state) or
            self._right.can_stop_polling(now, right_state))


class _AllOf(TerminationCondition):
  def __init__(self, left: TerminationCondition, right: TerminationCondition):
    self._left = left
    self._right = right

  def for_new_input(self, now: Timestamp, input_element: Any):
    return (self._left.for_new_input(now, input_element),
            self._right.for_new_input(now, input_element))

  def on_seen_new_output(self, now, state):
    left_state, right_state = state
    return (self._left.on_seen_new_output(now, left_state),
            self._right.on_seen_new_output(now, right_state))

  def on_poll_complete(self, state):
    left_state, right_state = state
    return (self._left.on_poll_complete(left_state),
            self._right.on_poll_complete(right_state))

  def can_stop_polling(self, now, state):
    left_state, right_state = state
    return (self._left.can_stop_polling(now, left_state) and
            self._right.can_stop_polling(now, right_state))


def _duration_to_secs(duration) -> float:
  """Converts a Duration/int/float into seconds."""
  if isinstance(duration, Duration):
    return duration.micros / 1_000_000.0
  if isinstance(duration, (int, float)):
    return float(duration)
  raise TypeError('Duration must be apache_beam Duration, int, or float')


def _validate_poll_interval_secs(duration) -> float:
  """Validates and returns a positive poll interval in seconds."""
  seconds = _duration_to_secs(duration)
  if not math.isfinite(seconds) or seconds <= 0:
    raise ValueError('poll_interval must be a positive finite duration')
  return seconds


def never() -> TerminationCondition:
  """Never terminates unless ``poll_result.complete`` is true."""
  return _NeverTermination()


def after_total_of(duration) -> TerminationCondition:
  """Terminate after total wall-clock time since the first poll."""
  return _AfterTotalOf(_duration_to_secs(duration))


def after_time_since_new_output(duration) -> TerminationCondition:
  """Terminate after no new outputs are seen for the given duration."""
  return _AfterTimeSinceNewOutput(_duration_to_secs(duration))


def either_of(c1: TerminationCondition, c2: TerminationCondition):
  """Terminate when either condition is satisfied."""
  return _EitherOf(c1, c2)


def all_of(c1: TerminationCondition, c2: TerminationCondition):
  """Terminate when both conditions are satisfied."""
  return _AllOf(c1, c2)
