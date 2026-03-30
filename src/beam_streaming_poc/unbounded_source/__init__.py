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

"""UnboundedSource SDF Wrapper.

Wraps legacy ``UnboundedSource`` instances as Splittable DoFns, enabling
them to run on portable runners.
"""

from beam_streaming_poc.unbounded_source.restriction import CheckpointMark
from beam_streaming_poc.unbounded_source.restriction import NOOP_CHECKPOINT_MARK
from beam_streaming_poc.unbounded_source.restriction import UnboundedReader
from beam_streaming_poc.unbounded_source.restriction import UnboundedSource
from beam_streaming_poc.unbounded_source.restriction import ValueWithRecordId
from beam_streaming_poc.unbounded_source.restriction import _SDFUnboundedSourceRestriction
from beam_streaming_poc.unbounded_source.restriction import _SDFUnboundedSourceRestrictionCoder
from beam_streaming_poc.unbounded_source.tracker import _SDFUnboundedSourceRestrictionTracker
from beam_streaming_poc.unbounded_source.wrapper import ReadFromUnboundedSourceFn
from beam_streaming_poc.unbounded_source.wrapper import _put_cached_reader
from beam_streaming_poc.unbounded_source.wrapper import _SDFUnboundedSourceDoFn
from beam_streaming_poc.unbounded_source.empty_source import EmptyUnboundedSource

__all__ = [
    'CheckpointMark',
    'NOOP_CHECKPOINT_MARK',
    'UnboundedReader',
    'UnboundedSource',
    'ValueWithRecordId',
    'ReadFromUnboundedSourceFn',
    'EmptyUnboundedSource',
]
