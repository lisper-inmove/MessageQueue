import asyncio

import redis
from redis import RedisCluster
from redis.cluster import ClusterNode

from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)

from redis.typing import (
    AbsExpiryT,
    AnyKeyT,
    BitfieldOffsetT,
    ChannelT,
    CommandsProtocol,
    ConsumerT,
    EncodableT,
    ExpiryT,
    FieldT,
    GroupT,
    KeysT,
    KeyT,
    PatternT,
    ScriptTextT,
    StreamIdT,
    TimeoutSecT,
    ZScoreBoundT,
)

ResponseT = Union[Awaitable, Any]


class AredisCluster:

    def __init__(self, host, port, password, startup_nodes=[]):
        self.host = host
        self.port = port
        self.password = password
        self.__init_startup_nodes(startup_nodes)
        self.loop = asyncio.get_running_loop()

    def __init_startup_nodes(self, startup_nodes):
        self.startup_nodes = []
        for host, port in startup_nodes:
            self.startup_nodes.append(
                ClusterNode(host, int(port))
            )

    def __connect(self):
        url = f"redis://default:{self.password}@{self.host}:{self.port}/0"
        self.client = RedisCluster.from_url(
            url,
            startup_nodes=self.startup_nodes
        )

    async def connect(self):
        return await self.__wrap_run(self.__connect)

    async def set(self, key, value):
        return await self.__wrap_run(self.client.set, key, value)

    async def get(self, key):
        value = await self.__wrap_run(self.client.get, key)
        return value

    async def xgroup_create(
            self,
            name: KeyT,
            groupname: GroupT,
            id: StreamIdT = "$",
            mkstream: bool = False,
            entries_read: Optional[int] = None,
    ):
        try:
            await self.__wrap_run(
                self.client.xgroup_create, name, groupname, id, mkstream, entries_read
            )
        except redis.exceptions.ResponseError as ex:
            if str(ex) == "BUSYGROUP Consumer Group name already exists":
                return
            raise ex
        except Exception as ex:
            raise ex

    async def xread(
            self,
            streams: Dict[KeyT, StreamIdT],
            count: Union[int, None] = None,
            block: Union[int, None] = None,
    ) -> ResponseT:
        return await self.__wrap_run(
            self.client.xread, streams, count, block
        )

    async def xreadgroup(
        self,
        groupname: str,
        consumername: str,
        streams: Dict[KeyT, StreamIdT],
        count: Union[int, None] = None,
        block: Union[int, None] = None,
        noack: bool = False,
    ) -> ResponseT:
        return await self.__wrap_run(
            self.client.xreadgroup, groupname, consumername,
            streams, count, block, noack
        )

    async def xadd(
        self,
        name: KeyT,
        fields: Dict[FieldT, EncodableT],
        id: StreamIdT = "*",
        maxlen: Union[int, None] = None,
        approximate: bool = True,
        nomkstream: bool = False,
        minid: Union[StreamIdT, None] = None,
        limit: Union[int, None] = None,
    ) -> ResponseT:
        return await self.__wrap_run(
            self.client.xadd, name, fields, id, maxlen,
            approximate, nomkstream, minid, limit
        )

    async def xdel(
            self,
            name: KeyT,
            *ids: StreamIdT
    ) -> ResponseT:
        return await self.__wrap_run(
            self.client.xdel, name, *ids
        )

    async def xack(self, name: KeyT, groupname: GroupT, *ids: StreamIdT) -> ResponseT:
        return await self.__wrap_run(
            self.client.xack, name, groupname, *ids
        )

    async def xpending(self, name: KeyT, groupname: GroupT) -> ResponseT:
        return await self.__wrap_run(
            self.client.xpending, name, groupname
        )

    async def xpending_range(
        self,
        name: KeyT,
        groupname: GroupT,
        min: StreamIdT,
        max: StreamIdT,
        count: int,
        consumername: Union[ConsumerT, None] = None,
        idle: Union[int, None] = None,
    ) -> ResponseT:
        return await self.__wrap_run(
            self.client.xpending_range,
            name, groupname, min, max, count, consumername,
            idle
        )

    async def xclaim(
        self,
        name: KeyT,
        groupname: GroupT,
        consumername: ConsumerT,
        min_idle_time: int,
        message_ids: Union[List[StreamIdT], Tuple[StreamIdT]],
        idle: Union[int, None] = None,
        time: Union[int, None] = None,
        retrycount: Union[int, None] = None,
        force: bool = False,
        justid: bool = False,
    ) -> ResponseT:
        return await self.__wrap_run(
            self.client.xclaim,
            name,
            groupname,
            min_idle_time,
            message_ids,
            idle,
            time,
            retrycount,
            force,
            justid
        )

    async def xautoclaim(
        self,
        name: KeyT,
        groupname: GroupT,
        consumername: ConsumerT,
        min_idle_time: int,
        start_id: StreamIdT = "0-0",
        count: Union[int, None] = None,
        justid: bool = False,
    ) -> ResponseT:
        return await self.__wrap_run(
            self.client.xautoclaim,
            name,
            groupname,
            consumername,
            min_idle_time,
            start_id,
            count,
            justid
        )

    async def __wrap_run(self, func, *args):
        return await self.loop.run_in_executor(None, func, *args)


if __name__ == "__main__":

    async def main():
        cluster = AredisCluster(
            "redis1",
            7001,
            startup_nodes=[
                ("redis1", 7001),
                ("redis2", 7002),
                ("redis3", 7003),
                ("redis4", 7004),
                ("redis5", 7005),
                ("redis6", 7006),
            ]
        )
        await cluster.connect()
        await cluster.set("key", 100)
        coros = [
            cluster.get("key"),
        ]
        await asyncio.gather(*coros)

    asyncio.run(main())
