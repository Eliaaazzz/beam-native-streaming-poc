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

"""PollResult container and PollFn protocol for the Watch transform."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from typing import Iterable
from typing import Optional
from typing import Tuple

from apache_beam.utils.timestamp import Timestamp


@dataclass(frozen=True)
class PollResult:
  """The result of one growth poll.

  Attributes:
    outputs: Newly discovered output values.
    watermark: Optional watermark candidate for this poll.
    complete: When true, polling for this input is finished.
  """
  outputs: Tuple[Any, ...] = ()
  watermark: Optional[Timestamp] = None
  complete: bool = False

  @classmethod
  def of(
      cls,
      outputs: Iterable[Any],
      watermark: Optional[Timestamp] = None,
      complete: bool = False) -> 'PollResult':
    return cls(outputs=tuple(outputs), watermark=watermark, complete=complete)

  @classmethod
  def empty(
      cls,
      watermark: Optional[Timestamp] = None,
      complete: bool = False) -> 'PollResult':
    return cls(outputs=(), watermark=watermark, complete=complete)
