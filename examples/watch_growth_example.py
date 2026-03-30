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

"""Runnable example: Watch transform for polling-based growth detection.

This example simulates an external system that produces new files over
time.  The ``Watch`` transform polls for new files and emits each newly
discovered filename exactly once.

Usage::

    python examples/watch_growth_example.py
"""

import logging
import time

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from beam_streaming_poc.watch import (
    PollResult,
    Watch,
    after_total_of,
)


# -- Simulated external system --------------------------------------------

_DISCOVERED_FILES = []


def _simulate_file_arrivals():
  """Background thread that "creates" files over time."""
  import threading

  def _produce():
    for i in range(5):
      time.sleep(0.3)
      _DISCOVERED_FILES.append(f'file_{i}.csv')

  t = threading.Thread(target=_produce, daemon=True)
  t.start()


# -- PollFn ---------------------------------------------------------------

def poll_for_files(seed):
  """Poll the simulated system for new files.

  Args:
    seed: The seed element (directory path in a real scenario).

  Returns:
    A ``PollResult`` containing newly discovered filenames.
  """
  current = list(_DISCOVERED_FILES)
  if not current:
    return PollResult.empty()
  return PollResult.of(current, complete=False)


# -- Pipeline -------------------------------------------------------------

def run():
  logging.basicConfig(level=logging.INFO)
  options = PipelineOptions()

  _simulate_file_arrivals()

  with beam.Pipeline(options=options) as p:
    (
        p
        | 'Seeds' >> beam.Create(['/data/incoming'])
        | 'WatchFiles' >> Watch.growth_of(poll_for_files)
            .with_poll_interval(0.5)
            .with_termination_per_input(after_total_of(3.0))
        | 'Log' >> beam.Map(
            lambda kv: logging.info('Seed=%s, NewFile=%s', kv[0], kv[1]))
    )


if __name__ == '__main__':
  run()
