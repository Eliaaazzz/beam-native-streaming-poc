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

"""Runnable example: reading from an UnboundedSource via the SDF wrapper.

This example defines a simple ``CountingUnboundedSource`` that emits
sequential integers and demonstrates how to wire it through the
``ReadFromUnboundedSourceFn`` transform.

Usage::

    python examples/unbounded_source_example.py
"""

import logging
import time

import apache_beam as beam
from apache_beam import coders
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.utils.timestamp import MAX_TIMESTAMP
from apache_beam.utils.timestamp import Timestamp

from beam_streaming_poc.unbounded_source import (
    CheckpointMark,
    UnboundedReader,
    UnboundedSource,
    ReadFromUnboundedSourceFn,
)


# -- Custom source that emits integers 0..max_count-1 --------------------

class _CountingCheckpointMark(CheckpointMark):
  """Stores the next index to emit."""
  def __init__(self, index):
    self.index = index

  def finalize_checkpoint(self):
    pass


class _CountingCheckpointMarkCoder(coders.Coder):
  def encode(self, value):
    return str(value.index).encode('utf-8')

  def decode(self, encoded):
    return _CountingCheckpointMark(int(encoded.decode('utf-8')))

  def is_deterministic(self):
    return True


class _CountingReader(UnboundedReader):
  """Emits integers ``[start, max_count)`` with a 0.1 s delay per element."""
  def __init__(self, source, start):
    self._source = source
    self._index = start
    self._current = None

  def start(self):
    return self.advance()

  def advance(self):
    if self._index >= self._source.max_count:
      return False
    self._current = self._index
    self._index += 1
    time.sleep(0.05)  # simulate latency
    return True

  def get_current(self):
    return self._current

  def get_current_timestamp(self):
    return Timestamp.of(self._current)

  def get_watermark(self):
    if self._index >= self._source.max_count:
      return MAX_TIMESTAMP
    return Timestamp.of(self._index - 1)

  def get_checkpoint_mark(self):
    return _CountingCheckpointMark(self._index)

  def close(self):
    pass


class CountingUnboundedSource(UnboundedSource):
  """Produces integers ``[0, max_count)``."""
  def __init__(self, max_count=10):
    self.max_count = max_count

  def create_reader(self, pipeline_options, checkpoint_mark):
    start = checkpoint_mark.index if checkpoint_mark else 0
    return _CountingReader(self, start)

  def get_checkpoint_mark_coder(self):
    return _CountingCheckpointMarkCoder()

  def split(self, desired_num_splits=1):
    return [self]


# -- Pipeline -------------------------------------------------------------

def run():
  logging.basicConfig(level=logging.INFO)
  options = PipelineOptions(['--streaming'])

  with beam.Pipeline(options=options) as p:
    (
        p
        | 'CreateSource' >> beam.Create([CountingUnboundedSource(max_count=10)])
        | 'ReadViaSDF' >> ReadFromUnboundedSourceFn()
        | 'Log' >> beam.Map(lambda x: logging.info('Got element: %s', x))
    )


if __name__ == '__main__':
  run()
