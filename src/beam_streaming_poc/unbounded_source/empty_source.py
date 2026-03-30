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

"""EmptyUnboundedSource sentinel for primary/residual splits.

The primary retains the active checkpoint while the residual is an empty,
immediately-done source.  Used in the ``try_split`` path of
``_SDFUnboundedSourceRestrictionTracker``.
"""

from apache_beam import coders
from apache_beam.utils.timestamp import MAX_TIMESTAMP

from beam_streaming_poc.unbounded_source.restriction import CheckpointMark
from beam_streaming_poc.unbounded_source.restriction import NOOP_CHECKPOINT_MARK
from beam_streaming_poc.unbounded_source.restriction import UnboundedReader
from beam_streaming_poc.unbounded_source.restriction import UnboundedSource


class _EmptyCheckpointMarkCoder(coders.Coder):
  """Trivial coder for ``NOOP_CHECKPOINT_MARK``."""
  def encode(self, value):
    return b''

  def decode(self, encoded):
    return NOOP_CHECKPOINT_MARK

  def is_deterministic(self):
    return True


class _EmptyUnboundedReader(UnboundedReader):
  """A reader that is immediately exhausted."""
  def start(self):
    return False

  def advance(self):
    return False

  def get_current(self):
    raise StopIteration('EmptyUnboundedReader has no elements')

  def get_watermark(self):
    return MAX_TIMESTAMP

  def get_checkpoint_mark(self):
    return NOOP_CHECKPOINT_MARK


class EmptyUnboundedSource(UnboundedSource):
  """Sentinel source that produces no elements and is immediately done.

  Used as the residual in primary/residual splits so the runner can
  recognise the split is complete without additional I/O.
  """
  _CODER = _EmptyCheckpointMarkCoder()

  def create_reader(self, pipeline_options, checkpoint_mark):
    return _EmptyUnboundedReader()

  def split(self, desired_num_splits=1, pipeline_options=None):
    return [self]

  def get_checkpoint_mark_coder(self):
    return self._CODER

  def is_bounded(self):
    return False
