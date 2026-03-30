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

"""Watch Transform -- polling-based growth detection.

Monitors external resources for new elements via periodic polling with
deduplication and composable termination conditions.
"""

from beam_streaming_poc.watch.poll import PollResult
from beam_streaming_poc.watch.growth_state import GrowthState
from beam_streaming_poc.watch.growth_state import NonPollingGrowthState
from beam_streaming_poc.watch.growth_state import PollingGrowthState
from beam_streaming_poc.watch.growth_state import EMPTY_STATE
from beam_streaming_poc.watch.growth_state import _GrowthStateCoder
from beam_streaming_poc.watch.growth_state import _GrowthStateTracker
from beam_streaming_poc.watch.termination import TerminationCondition
from beam_streaming_poc.watch.termination import never
from beam_streaming_poc.watch.termination import after_total_of
from beam_streaming_poc.watch.termination import after_time_since_new_output
from beam_streaming_poc.watch.termination import either_of
from beam_streaming_poc.watch.termination import all_of
from beam_streaming_poc.watch.growth_fn import WatchGrowthFn
from beam_streaming_poc.watch.growth_fn import Watch

__all__ = [
    'PollResult',
    'GrowthState',
    'NonPollingGrowthState',
    'PollingGrowthState',
    'EMPTY_STATE',
    'TerminationCondition',
    'WatchGrowthFn',
    'Watch',
    'never',
    'after_total_of',
    'after_time_since_new_output',
    'either_of',
    'all_of',
]
