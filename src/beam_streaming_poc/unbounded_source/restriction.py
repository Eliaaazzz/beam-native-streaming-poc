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

"""Restriction, Coder, and base classes for the UnboundedSource SDF wrapper.

Contains:
- ``CheckpointMark`` / ``NOOP_CHECKPOINT_MARK``
- ``UnboundedSource`` / ``UnboundedReader`` ABCs
- ``_SDFUnboundedSourceRestriction`` encapsulating checkpoint state
- ``_SDFUnboundedSourceRestrictionCoder`` for checkpoint serialization
"""

import logging
import struct
import time

from apache_beam import coders
from apache_beam.internal import pickler
from apache_beam.io.iobase import SourceBase
from apache_beam.metrics import Metrics
from apache_beam.utils.timestamp import MIN_TIMESTAMP
from apache_beam.utils.timestamp import Timestamp

LOG = logging.getLogger(__name__)


# ===================================================================
#  UnboundedSource / UnboundedReader base classes
# ===================================================================

class CheckpointMark:
  """Marker representing the progress of an UnboundedReader.

  Mirrors Java ``UnboundedSource.CheckpointMark``.
  """
  def finalize_checkpoint(self):
    """Called when the runner commits this checkpoint."""
    pass


NOOP_CHECKPOINT_MARK = CheckpointMark()


class UnboundedSource(SourceBase):
  """A source that reads an unbounded amount of input.

  Subclasses must implement ``create_reader`` and
  ``get_checkpoint_mark_coder``.
  """
  def create_reader(self, pipeline_options, checkpoint_mark):
    """Create a reader, optionally resuming from *checkpoint_mark*.

    Mirrors Java ``UnboundedSource.createReader(PipelineOptions,
    CheckpointMark)``.  *pipeline_options* may be ``None`` in test
    environments.
    """
    raise NotImplementedError

  def split(self, desired_num_splits=1):
    """Split into sub-sources for parallel reading. Default: ``[self]``."""
    return [self]

  def get_checkpoint_mark_coder(self):
    """Return a ``Coder`` for the ``CheckpointMark`` type."""
    raise NotImplementedError

  def is_bounded(self):
    return False

  def default_output_coder(self):
    return coders.registry.get_coder(object)


class UnboundedReader:
  """Reads records from an ``UnboundedSource``."""

  BACKLOG_UNKNOWN = -1

  def start(self):
    """Initialise and advance to the first record.

    Returns ``True`` if a record is available, ``False`` otherwise.
    """
    raise NotImplementedError

  def advance(self):
    """Advance to the next record.

    Returns ``True`` if a record is available, ``False`` otherwise.
    """
    raise NotImplementedError

  def get_current(self):
    """Return the current element."""
    raise NotImplementedError

  def get_current_timestamp(self):
    """Return the timestamp of the current element."""
    return Timestamp(0)

  def get_watermark(self):
    """Return a watermark (lower bound on future timestamps)."""
    return MIN_TIMESTAMP

  def get_checkpoint_mark(self):
    """Return a ``CheckpointMark`` for the current reader position."""
    return NOOP_CHECKPOINT_MARK

  def get_split_backlog_bytes(self):
    """Return the remaining backlog in bytes, if known."""
    return self.BACKLOG_UNKNOWN

  def close(self):
    """Release resources held by this reader."""
    pass


# ===================================================================
#  Restriction
# ===================================================================

class _SDFUnboundedSourceRestriction:
  """Wraps an ``UnboundedSource`` + checkpoint for the SDF framework.

  Analogous to ``_SDFBoundedSourceRestriction`` (``iobase.py``), which
  wraps ``SourceBundle`` + ``RangeTracker``.

  All fields are persistent (serialized): ``source``, ``checkpoint``,
  ``is_done``, ``watermark``.  There are no transient reader fields here;
  the DoFn owns the reader lifecycle (mirrors Java's ``cachedReaders``
  being on ``UnboundedSourceAsSDFWrapperFn``, not on the tracker).
  """
  def __init__(self, source, checkpoint=None, is_done=False, watermark=None):
    if not isinstance(source, UnboundedSource):
      raise TypeError(
          'Expected UnboundedSource, got %s' % type(source).__name__)
    self.source = source
    self.checkpoint = checkpoint
    self.is_done = is_done
    self.watermark = watermark if watermark is not None else MIN_TIMESTAMP

  def __repr__(self):
    done = ', done' if self.is_done else ''
    return ('_SDFUnboundedSourceRestriction(source=%s, checkpoint=%s, '
            'watermark=%s%s)') % (
        type(self.source).__name__, self.checkpoint, self.watermark, done)

  def __reduce__(self):
    return (self.__class__, (
        self.source, self.checkpoint, self.is_done, self.watermark))


# ===================================================================
#  Restriction Coder
# ===================================================================

class _SDFUnboundedSourceRestrictionCoder(coders.Coder):
  """Coder for ``_SDFUnboundedSourceRestriction``.

  Binary layout: ``[flags:1][src_len:4][cp_len:4][wm_micros:8][src][cp]``

  Flags: bit 0 = has_checkpoint, bit 1 = is_done.
  """
  def encode(self, value):
    t0 = time.time()
    source_bytes = pickler.dumps(value.source)

    if value.checkpoint is not None:
      cp_coder = value.source.get_checkpoint_mark_coder()
      cp_bytes = cp_coder.encode(value.checkpoint)
      has_cp = True
    else:
      cp_bytes = b''
      has_cp = False

    wm_micros = int(value.watermark.micros)
    flags = (int(has_cp) << 0) | (int(value.is_done) << 1)
    header = struct.pack(
        '>BIIq', flags, len(source_bytes), len(cp_bytes), wm_micros)
    elapsed_us = int((time.time() - t0) * 1_000_000)
    Metrics.distribution(
        'UnboundedSourceSDF', 'checkpoint_encode_time_us').update(elapsed_us)
    return header + source_bytes + cp_bytes

  def decode(self, encoded):
    flags, source_len, cp_len, wm_micros = struct.unpack_from(
        '>BIIq', encoded, 0)
    has_cp = bool(flags & 1)
    is_done = bool(flags & 2)

    offset = 17  # 1 + 4 + 4 + 8
    source = pickler.loads(encoded[offset:offset + source_len])
    offset += source_len

    if has_cp:
      cp_coder = source.get_checkpoint_mark_coder()
      checkpoint = cp_coder.decode(encoded[offset:offset + cp_len])
    else:
      checkpoint = None

    watermark = Timestamp(micros=wm_micros)
    return _SDFUnboundedSourceRestriction(
        source, checkpoint, is_done, watermark)

  def is_deterministic(self):
    return False
