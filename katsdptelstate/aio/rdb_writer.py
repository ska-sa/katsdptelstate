################################################################################
# Copyright (c) 2018-2023, National Research Foundation (Square Kilometre Array)
#
# Licensed under the BSD 3-Clause License (the "License"); you may not use
# this file except in compliance with the License. You may obtain a copy
# of the License at
#
#   https://opensource.org/licenses/BSD-3-Clause
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import logging
from typing import Iterable, Union, Optional

from redis import asyncio as aioredis

from ..rdb_writer_base import RDBWriterBase
from ..utils import ensure_binary
from .telescope_state import TelescopeState
from .backend import Backend


logger = logging.getLogger(__name__)


class RDBWriter(RDBWriterBase):
    __doc__ = RDBWriterBase.__doc__

    async def save(self, client: Union[TelescopeState, Backend, aioredis.Redis],
                   keys: Optional[Iterable[Union[str, bytes]]] = None) -> None:
        """Save a specified subset of keys from a Redis DB to the RDB dump file.

        This is a very limited RDB dump utility. It takes a Redis database
        either from a high-level :class:`~katsdptelstate.TelescopeState` object,
        its low-level backend or a compatible redis client. Missing or broken
        keys are skipped and logged without raising an exception.

        Parameters
        ----------
        client : :class:`~katsdptelstate.aio.telescope_state.TelescopeState` or :class:`~katsdptelstate.aio.telescope_state.Backend` or :class:`~aioredis.Redis`-like
            A telstate, backend, or Redis-compatible client instance supporting keys() and dump()
        keys : iterable of str or bytes, optional
            The keys to extract from Redis and include in the dump.
            Keys that don't exist will not raise an Exception, only a log message.
            None (default) includes all keys.
        """  # noqa: E501
        if isinstance(client, TelescopeState):
            client = client.backend
        if keys is None:
            logger.info("No keys specified - dumping entire database")
            keys = await client.keys(b'*')
        for key in keys:
            key = ensure_binary(key)
            dumped_value = await client.dump(key)
            self.write_key(key, dumped_value)
