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

"""GrowthState hierarchy, Coder, and Tracker for the Watch transform.

- ``GrowthState`` base with ``PollingGrowthState`` and ``NonPollingGrowthState``
- ``_GrowthStateCoder`` for checkpoint serialization
- ``_GrowthStateTracker`` with three-case ``try_split``
"""

from __future__ import annotations

import abc
import collections
import io
import struct
from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import Callable
from typing import Optional
from typing import cast

from apache_beam import coders
from apache_beam.internal import pickler
from apache_beam.io.iobase import RestrictionProgress
from apache_beam.io.iobase import RestrictionTracker
from apache_beam.utils.timestamp import Timestamp


# ===================================================================
#  Growth state hierarchy
# ===================================================================

class GrowthState(abc.ABC):
  """Abstract base for Watch growth restriction states."""
  pass


@dataclass
class NonPollingGrowthState(GrowthState):
  """Bounded replay state: holds prior poll outputs to re-emit.

  Created as the *primary* restriction after a checkpoint of a
  ``PollingGrowthState`` (try_split Case 3).  Also used as
  ``EMPTY_STATE`` sentinel (with empty outputs and no watermark).
  """
  pending_outputs: tuple = ()    # tuple of (Timestamp, value)
  pending_watermark: Optional[Timestamp] = None


@dataclass
class PollingGrowthState(GrowthState):
  """Unbounded polling state: actively polls for new outputs.

  Attributes:
    completed: Mapping from 16-byte dedup hash -> discovery Timestamp.
    poll_watermark: Latest watermark for monotonic reporting.
    termination_state: Opaque state for the TerminationCondition.
  """
  completed: collections.OrderedDict = field(
      default_factory=collections.OrderedDict)
  poll_watermark: Optional[Timestamp] = None
  termination_state: Any = None


EMPTY_STATE = NonPollingGrowthState()

# Type discriminators for binary coder.
_TYPE_POLLING = 0
_TYPE_NON_POLLING = 1


def _max_nullable(
    a: Optional[Timestamp],
    b: Optional[Timestamp]) -> Optional[Timestamp]:
  if a is None:
    return b
  if b is None:
    return a
  return max(a, b)


# ===================================================================
#  Coder
# ===================================================================

class _GrowthStateCoder(coders.Coder):
  """Structured binary coder for the ``GrowthState`` hierarchy.

  Binary layout::

      [type:1]

      type=0 (Polling):
          [completed_count:4]([hash:16][ts_micros:8])...
          [term_len:4][term_bytes]
          [has_wm:1]([wm_micros:8])?

      type=1 (NonPolling):
          [output_count:4]([ts_micros:8][val_len:4][val_bytes])...
          [has_wm:1]([wm_micros:8])?
  """

  def __init__(self, output_coder: Optional[coders.Coder] = None):
    self._output_coder = output_coder

  def encode(self, value: GrowthState) -> bytes:
    buf = io.BytesIO()

    if isinstance(value, PollingGrowthState):
      buf.write(struct.pack('>B', _TYPE_POLLING))
      self._encode_polling(value, buf)
    elif isinstance(value, NonPollingGrowthState):
      buf.write(struct.pack('>B', _TYPE_NON_POLLING))
      self._encode_non_polling(value, buf)
    else:
      raise ValueError('Unknown GrowthState type: %s' % type(value))

    return buf.getvalue()

  def _encode_polling(self, state: PollingGrowthState, buf: io.BytesIO):
    items = list(state.completed.items())
    buf.write(struct.pack('>I', len(items)))
    for hash_key, ts in items:
      buf.write(hash_key)
      buf.write(struct.pack('>q', ts.micros))

    term_bytes = pickler.dumps(state.termination_state)
    buf.write(struct.pack('>I', len(term_bytes)))
    buf.write(term_bytes)

    has_wm = state.poll_watermark is not None
    buf.write(struct.pack('>?', has_wm))
    if has_wm:
      buf.write(struct.pack('>q', state.poll_watermark.micros))

  def _encode_non_polling(
      self, state: NonPollingGrowthState, buf: io.BytesIO):
    buf.write(struct.pack('>I', len(state.pending_outputs)))
    for ts, val in state.pending_outputs:
      buf.write(struct.pack('>q', ts.micros))
      if self._output_coder is not None:
        val_bytes = self._output_coder.encode(val)
      else:
        val_bytes = pickler.dumps(val)
      buf.write(struct.pack('>I', len(val_bytes)))
      buf.write(val_bytes)

    has_wm = state.pending_watermark is not None
    buf.write(struct.pack('>?', has_wm))
    if has_wm:
      buf.write(struct.pack('>q', state.pending_watermark.micros))

  def decode(self, encoded: bytes) -> GrowthState:
    off = 0
    type_disc, = struct.unpack_from('>B', encoded, off); off += 1

    if type_disc == _TYPE_POLLING:
      return self._decode_polling(encoded, off)
    elif type_disc == _TYPE_NON_POLLING:
      return self._decode_non_polling(encoded, off)
    else:
      raise ValueError(
          'Unknown GrowthState type discriminator: %d' % type_disc)

  def _decode_polling(self, encoded: bytes, off: int) -> PollingGrowthState:
    count, = struct.unpack_from('>I', encoded, off); off += 4
    completed = collections.OrderedDict()
    for _ in range(count):
      h = encoded[off:off + 16]; off += 16
      ts_micros, = struct.unpack_from('>q', encoded, off); off += 8
      completed[h] = Timestamp(micros=ts_micros)

    term_len, = struct.unpack_from('>I', encoded, off); off += 4
    term_state = pickler.loads(encoded[off:off + term_len]); off += term_len

    has_wm, = struct.unpack_from('>?', encoded, off); off += 1
    wm = None
    if has_wm:
      wm_micros, = struct.unpack_from('>q', encoded, off); off += 8
      wm = Timestamp(micros=wm_micros)

    return PollingGrowthState(
        completed=completed, poll_watermark=wm,
        termination_state=term_state)

  def _decode_non_polling(
      self, encoded: bytes, off: int) -> NonPollingGrowthState:
    count, = struct.unpack_from('>I', encoded, off); off += 4
    outputs = []
    for _ in range(count):
      ts_micros, = struct.unpack_from('>q', encoded, off); off += 8
      val_len, = struct.unpack_from('>I', encoded, off); off += 4
      val_bytes = encoded[off:off + val_len]; off += val_len
      if self._output_coder is not None:
        val = self._output_coder.decode(val_bytes)
      else:
        val = pickler.loads(val_bytes)
      outputs.append((Timestamp(micros=ts_micros), val))

    has_wm, = struct.unpack_from('>?', encoded, off); off += 1
    wm = None
    if has_wm:
      wm_micros, = struct.unpack_from('>q', encoded, off); off += 8
      wm = Timestamp(micros=wm_micros)

    return NonPollingGrowthState(
        pending_outputs=tuple(outputs), pending_watermark=wm)


# ===================================================================
#  GrowthStateTracker
# ===================================================================

class _GrowthStateTracker(RestrictionTracker):
  """Three-case tracker mirroring Java ``Watch.GrowthTracker``.

  The claim position is a tuple ``(outputs, watermark, termination_state)``
  where ``outputs`` is a tuple of ``(Timestamp, value)`` pairs.
  """

  def __init__(
      self,
      state: GrowthState,
      hash_fn: Callable[[Any], bytes],
      max_completed_size: Optional[int] = None):
    self._state = state
    self._hash_fn = hash_fn
    self._max_completed_size = max_completed_size

    self._claimed_outputs = None
    self._claimed_watermark = None
    self._claimed_termination_state = None
    self._claimed_hashes = None

    self._should_stop = False

  def current_restriction(self):
    return self._state

  def current_progress(self):
    return RestrictionProgress(fraction=0.0)

  def try_claim(self, position):
    if self._should_stop:
      return False

    outputs, watermark, termination_state = position

    new_hashes = collections.OrderedDict()
    for ts, value in outputs:
      h = self._hash_fn(value)
      new_hashes[h] = ts

    if isinstance(self._state, PollingGrowthState):
      if not self._state.completed.keys().isdisjoint(new_hashes.keys()):
        return False
    elif isinstance(self._state, NonPollingGrowthState):
      expected_hashes = set()
      for ts, value in self._state.pending_outputs:
        expected_hashes.add(self._hash_fn(value))
      if expected_hashes != set(new_hashes.keys()):
        return False

    self._should_stop = True
    self._claimed_outputs = outputs
    self._claimed_watermark = watermark
    self._claimed_termination_state = termination_state
    self._claimed_hashes = new_hashes
    return True

  def try_split(self, fraction_of_remainder):
    """Three-case split mirroring Java ``GrowthTracker.trySplit()``.

    Case 1 (nothing claimed): primary=EMPTY, residual=current state.
    Case 2 (claimed + NonPolling): residual=EMPTY.
    Case 3 (claimed + Polling): primary=NonPolling(claimed),
            residual=Polling(merged completed).
    """
    del fraction_of_remainder

    if self._state is EMPTY_STATE and self._claimed_outputs is None:
      self._should_stop = True
      return None

    if self._claimed_outputs is None:
      residual = self._state
      self._state = EMPTY_STATE
    elif isinstance(self._state, NonPollingGrowthState):
      residual = EMPTY_STATE
    else:
      current_polling = cast(PollingGrowthState, self._state)

      new_completed = collections.OrderedDict(current_polling.completed)
      new_completed.update(self._claimed_hashes)

      if (self._max_completed_size is not None and
          len(new_completed) > self._max_completed_size):
        while len(new_completed) > self._max_completed_size:
          oldest_key = next(iter(new_completed))
          del new_completed[oldest_key]

      new_watermark = _max_nullable(
          current_polling.poll_watermark, self._claimed_watermark)

      residual = PollingGrowthState(
          completed=new_completed,
          poll_watermark=new_watermark,
          termination_state=self._claimed_termination_state)
      self._state = NonPollingGrowthState(
          pending_outputs=self._claimed_outputs,
          pending_watermark=self._claimed_watermark)

    self._should_stop = True
    return self._state, residual

  def check_done(self):
    if not self._should_stop:
      raise ValueError(
          'GrowthTracker: check_done called but neither try_claim() '
          'nor try_split() was called.')

  def is_bounded(self):
    return isinstance(self._state, NonPollingGrowthState)
