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

"""RestrictionTracker for the UnboundedSource SDF wrapper.

The RestrictionProvider and WatermarkEstimatorProvider have been merged
into ``_SDFUnboundedSourceDoFn`` (in ``wrapper.py``) so that
``pipeline_options`` is available in ``split()`` and ``create_reader()``.
"""

import logging

from apache_beam.io.iobase import RestrictionProgress
from apache_beam.io.iobase import RestrictionTracker
from apache_beam.utils.timestamp import MAX_TIMESTAMP

from beam_streaming_poc.unbounded_source.restriction import _SDFUnboundedSourceRestriction

LOG = logging.getLogger(__name__)


class _SDFUnboundedSourceRestrictionTracker(RestrictionTracker):
  """Pure-state-machine tracker for ``UnboundedSource`` SDF restrictions.

  ``try_claim(position)`` is a lightweight declaration -- it does NOT drive
  reader I/O.  The DoFn's ``process()`` method owns the reader and calls
  ``start()`` / ``advance()``; it updates ``restriction.checkpoint`` and
  ``restriction.watermark`` before each ``try_claim`` call so that
  ``try_split`` always has an up-to-date snapshot.

  This mirrors the Java PR #32044 design where ``tryClaim`` is decoupled
  from actual I/O and the tracker is a pure state machine.
  """
  def __init__(self, restriction):
    if not isinstance(restriction, _SDFUnboundedSourceRestriction):
      raise ValueError(
          'Expected _SDFUnboundedSourceRestriction, got %s' %
          type(restriction).__name__)
    self._restriction = restriction
    self._stopped = False
    self._claimed_count = 0

  def current_restriction(self):
    return self._restriction

  def try_claim(self, position):
    """Declare that the DoFn is about to process *position*.

    Returns ``True`` if the claim is accepted; ``False`` if the runner has
    requested a split or the restriction is done.
    """
    if self._stopped:
      return False
    if self._restriction.is_done:
      self._stopped = True
      return False
    self._claimed_count += 1
    return True

  def try_split(self, fraction_of_remainder):
    """Checkpoint the current reader position.

    Creates a "done" primary and a resumable residual carrying the latest
    checkpoint and watermark.
    """
    del fraction_of_remainder
    if self._restriction.is_done:
      return None
    if self._claimed_count == 0 and self._restriction.checkpoint is None:
      return None

    checkpoint = self._restriction.checkpoint
    watermark = self._restriction.watermark

    primary = _SDFUnboundedSourceRestriction(
        self._restriction.source,
        checkpoint=None,
        is_done=True,
        watermark=MAX_TIMESTAMP)

    residual = _SDFUnboundedSourceRestriction(
        self._restriction.source, checkpoint=checkpoint, watermark=watermark)

    self._restriction = _SDFUnboundedSourceRestriction(
        self._restriction.source,
        checkpoint=checkpoint,
        is_done=True,
        watermark=MAX_TIMESTAMP)
    self._stopped = True

    return primary, residual

  def check_done(self):
    if self._stopped or self._restriction.is_done:
      return
    raise ValueError(
        'RestrictionTracker has unclaimed work: process() returned '
        'without exhausting the source.  The process() method must '
        'yield ProcessContinuation.resume() when the source is '
        'temporarily idle, or continue until the restriction becomes done.')

  def current_progress(self):
    remaining = 0 if (self._stopped or self._restriction.is_done) else 1
    return RestrictionProgress(
        completed=self._claimed_count, remaining=remaining)

  def is_bounded(self):
    return False
