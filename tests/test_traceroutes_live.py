"""The live traceroute page groups each request with its responses and draws both directions."""

import time
from contextlib import asynccontextmanager

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from meshtastic.protobuf.mesh_pb2 import RouteDiscovery
from meshtastic.protobuf.mqtt_pb2 import ServiceEnvelope
from meshtastic.protobuf.portnums_pb2 import PortNum
from meshview import database, models, mqtt_database, mqtt_store, store
from meshview import web as mv_web
from meshview.web_api import api

INITIATOR, RELAY, TARGET = 0x10, 0x20, 0x40
GW_MID, GW_FAR = 0x30, 0x50
REQUEST_ID, REPLY_ID, OTHER_ID = 1000, 2000, 3000


def envelope(packet_id, src, dst, gateway, route, channel="LongFast", request_id=0):
    env = ServiceEnvelope(gateway_id=f"!{gateway:08x}", channel_id=channel)
    env.packet.id = packet_id
    setattr(env.packet, "from", src)
    env.packet.to = dst
    env.packet.decoded.portnum = PortNum.TRACEROUTE_APP
    env.packet.decoded.payload = route.SerializeToString()
    if request_id:
        env.packet.decoded.request_id = request_id
    else:
        env.packet.decoded.want_response = True
    return env


@asynccontextmanager
async def make_client(monkeypatch):
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    monkeypatch.setattr(database, "async_session", sessions)
    monkeypatch.setattr(mqtt_database, "async_session", sessions, raising=False)
    monkeypatch.setattr(mqtt_store, "MQTT_GATEWAY_CACHE", set())
    async with engine.begin() as conn:
        await conn.run_sync(models.Base.metadata.create_all)
    async with sessions() as session:
        # Every node but the far gateway is known by name
        for node_id in (INITIATOR, RELAY, TARGET, GW_MID):
            session.add(
                models.Node(
                    id=f"!{node_id:08x}", node_id=node_id, short_name=f"N{node_id:x}", role="ROUTER"
                )
            )
        await session.commit()

    # Request, as heard by a gateway one hop past the relay
    await mqtt_store.process_envelope(
        "t", envelope(REQUEST_ID, INITIATOR, TARGET, GW_MID, RouteDiscovery(route=[RELAY]))
    )
    # Response that made it all the way home: the initiator uplinked it
    home = RouteDiscovery(
        route=[RELAY], snr_towards=[20, 24], route_back=[RELAY], snr_back=[-128, 8]
    )
    await mqtt_store.process_envelope(
        "t", envelope(REPLY_ID, TARGET, INITIATOR, INITIATOR, home, request_id=REQUEST_ID)
    )
    # Same response heard on the way, before reaching the initiator
    partial = RouteDiscovery(route=[RELAY], snr_towards=[20, 24], route_back=[RELAY], snr_back=[4])
    await mqtt_store.process_envelope(
        "t", envelope(REPLY_ID, TARGET, INITIATOR, GW_FAR, partial, request_id=REQUEST_ID)
    )
    # Another traceroute on another channel
    await mqtt_store.process_envelope(
        "t",
        envelope(OTHER_ID, RELAY, TARGET, GW_FAR, RouteDiscovery(), channel="MediumFast"),
    )

    app = web.Application()
    app.add_routes(api.routes)
    app.add_routes(mv_web.routes)
    try:
        async with TestClient(TestServer(app)) as client:
            yield client
    finally:
        await engine.dispose()


async def get_json(client, query):
    res = await client.get("/api/traceroutes" + query)
    assert res.status == 200
    return await res.json()


@pytest.mark.asyncio
async def test_request_and_responses_share_the_request_id(monkeypatch):
    async with make_client(monkeypatch) as client:
        data = await get_json(client, "?minutes=30&channel=LongFast")
        rows = data["traceroutes"]
        assert [r["packet_id"] for r in rows] == [REQUEST_ID] * 3
        request, home, partial = rows

        assert request["done"] is False
        assert request["reply_packet_id"] is None
        assert (request["from"], request["to"]) == (INITIATOR, TARGET)
        assert request["towards"] == [INITIATOR, RELAY, GW_MID]
        assert request["back"] is None

        for reply in (home, partial):
            assert reply["done"] is True
            assert reply["reply_packet_id"] == REPLY_ID
            # Endpoints are those of the traceroute, not of the response packet
            assert (reply["from"], reply["to"]) == (INITIATOR, TARGET)
            assert reply["towards"] == [INITIATOR, RELAY, TARGET]
            assert reply["snr_towards"] == [5.0, 6.0]
        assert home["back"] == [TARGET, RELAY, INITIATOR]
        assert home["snr_back"] == [None, 2.0]
        assert partial["back"] == [TARGET, RELAY, GW_FAR]

        assert [r["gateway"] for r in rows] == [GW_MID, INITIATOR, GW_FAR]
        assert all(abs(data["now"] - r["time"]) < 60 for r in rows)
        assert data["last_id"] == partial["id"]
        assert set(data["nodes"]) == {str(n) for n in (INITIATOR, RELAY, TARGET, GW_MID)}
        assert data["nodes"][str(RELAY)] == {"short": "N20", "long": None, "role": "ROUTER"}


@pytest.mark.asyncio
async def test_polling_and_channel_filter(monkeypatch):
    async with make_client(monkeypatch) as client:
        everything = await get_json(client, "?channel=all")
        assert len(everything["traceroutes"]) == 4
        last = everything["last_id"]

        polled = await get_json(client, f"?channel=all&after={last}")
        assert polled["traceroutes"] == []
        assert polled["last_id"] == last

        other = await get_json(client, "?channel=MediumFast")
        assert [r["packet_id"] for r in other["traceroutes"]] == [OTHER_ID]
        assert (await get_json(client, "?channel=nope"))["traceroutes"] == []


@pytest.mark.asyncio
async def test_response_without_request_id_is_its_own_group(monkeypatch):
    async with make_client(monkeypatch) as client:
        last = (await get_json(client, "?channel=all"))["last_id"]
        route = RouteDiscovery(route=[RELAY], route_back=[RELAY], snr_back=[4, 8])
        env = envelope(4000, TARGET, INITIATOR, GW_FAR, route, request_id=1)
        env.packet.decoded.request_id = 0
        await mqtt_store.process_envelope("t", env)

        (row,) = (await get_json(client, f"?channel=all&after={last}"))["traceroutes"]
        assert row["packet_id"] == row["reply_packet_id"] == 4000
        assert (row["from"], row["to"]) == (INITIATOR, TARGET)
        # A complete return trip ends at the initiator, whoever uplinked it
        assert row["back"] == [TARGET, RELAY, INITIATOR]


@pytest.mark.asyncio
async def test_unreadable_rows_do_not_stop_the_feed(monkeypatch):
    async with make_client(monkeypatch) as client:
        last = (await get_json(client, "?channel=all"))["last_id"]
        now_us = int(time.time() * 1_000_000)
        async with database.async_session() as session:
            session.add_all(
                [
                    models.Packet(id=5000, portnum=PortNum.TRACEROUTE_APP, import_time_us=now_us),
                    models.Packet(
                        id=5001,
                        portnum=PortNum.TRACEROUTE_APP,
                        payload=b"\xff\xff",
                        import_time_us=now_us,
                    ),
                    models.Traceroute(
                        packet_id=5000, route=None, done=False, import_time_us=now_us
                    ),
                    models.Traceroute(packet_id=5000, route=b"", done=True, import_time_us=now_us),
                    models.Traceroute(
                        packet_id=5001, route=b"\xff\xff", done=True, import_time_us=now_us
                    ),
                    models.Traceroute(packet_id=5001, route=b"", done=True, import_time_us=now_us),
                    models.Traceroute(packet_id=9999, route=b"", done=True, import_time_us=now_us),
                ]
            )
            await session.commit()

        data = await get_json(client, f"?channel=all&after={last}")
        # Unparseable payloads still draw, under their own id; the rest are skipped
        assert [r["packet_id"] for r in data["traceroutes"]] == [5000, 5001]
        # No return-trip fields: nothing to draw on the way back
        assert [r["back"] for r in data["traceroutes"]] == [None, None]
        assert data["last_id"] == last + 5


@pytest.mark.asyncio
async def test_flood_naming_many_nodes_still_answers(monkeypatch):
    """A full answer of forged traceroutes names more nodes than one query can bind."""
    async with make_client(monkeypatch) as client:
        last = (await get_json(client, "?channel=all"))["last_id"]
        now_us = int(time.time() * 1_000_000)
        rows = store.MAX_LIVE_TRACEROUTE_ROWS
        first, last_hop = 0x1000000, 0x1000000 + rows * 16 - 1  # within the first answer
        async with database.async_session() as session:
            for node_id in (first, last_hop):
                session.add(models.Node(id=f"!{node_id:08x}", node_id=node_id, short_name="F"))
            for i in range(rows + 1):
                hops = [0x1000000 + i * 16 + h for h in range(16)]
                route = RouteDiscovery(route=hops[:8], route_back=hops[8:], snr_back=[0] * 8)
                session.add(models.Packet(id=10_000 + i, from_node_id=hops[0], to_node_id=hops[1]))
                session.add(
                    models.Traceroute(
                        packet_id=10_000 + i,
                        route=route.SerializeToString(),
                        done=True,
                        gateway_node_id=0x2000000 + i,
                        import_time_us=now_us,
                    )
                )
            await session.commit()

        data = await get_json(client, f"?channel=all&after={last}")
        assert len(data["traceroutes"]) == rows
        named = {n for r in data["traceroutes"] for n in r["towards"] + r["back"]}
        assert len(named) > 32_767
        assert set(data["nodes"]) == {str(first), str(last_hop)}
        # The row past the cap comes with the next poll
        rest = await get_json(client, f"?channel=all&after={data['last_id']}")
        assert len(rest["traceroutes"]) == 1


@pytest.mark.asyncio
async def test_window_excludes_old_rows(monkeypatch):
    async with make_client(monkeypatch) as client:
        real = store.get_traceroutes_live
        seen = []

        async def spy(since_us, after_id=None, channel=None):
            seen.append(since_us)
            return await real(since_us, after_id, channel)

        monkeypatch.setattr(store, "get_traceroutes_live", spy)
        data = await get_json(client, "?minutes=100000")
        # Clamped to the longest window
        assert data["now"] * 1_000_000 - seen[-1] == pytest.approx(
            api.MAX_LIVE_TRACEROUTE_MINUTES * 60_000_000, abs=1_000_000
        )
        future = int((data["now"] + 60) * 1_000_000)
        assert await real(future) == []

        data = await get_json(client, "?minutes=0")
        # At least one minute
        assert data["now"] * 1_000_000 - seen[-1] == pytest.approx(60_000_000, abs=1_000_000)


@pytest.mark.asyncio
async def test_store_window_and_node_batches(monkeypatch):
    async with make_client(monkeypatch):
        async with database.async_session() as session:
            session.add(models.Packet(id=7000, channel="X"))
            session.add_all(
                models.Traceroute(id=900 + t, packet_id=7000, route=b"", import_time_us=t)
                for t in (10, 11)
            )
            session.add_all(
                models.Node(id=f"n{i}", node_id=100_000 + i)
                for i in range(2 * store.NODE_ID_BATCH + 1)
            )
            await session.commit()

        assert [t.id for t in await store.get_traceroutes_live(10, channel="X")] == [911]
        assert [t.id for t in await store.get_traceroutes_live(0, 910, "X")] == [911]

        ids = [100_000 + i for i in range(2 * store.NODE_ID_BATCH + 1)]
        assert sorted(n.node_id for n in await store.get_nodes_by_ids(ids)) == ids


@pytest.mark.asyncio
async def test_menu_switch(monkeypatch):
    async with make_client(monkeypatch) as client:
        site_config = api.CONFIG.setdefault("site", {})
        # Existing config files have no such key: the page is in the menu
        monkeypatch.delitem(site_config, "tracelive", raising=False)
        site = (await (await client.get("/api/config")).json())["site"]
        assert site["tracelive"] == "true"
        monkeypatch.setitem(site_config, "tracelive", "False")
        site = (await (await client.get("/api/config")).json())["site"]
        assert site["tracelive"] == "false"


@pytest.mark.parametrize("query", ["?minutes=x", "?after=1.5", "?after=" + "9" * 30])
@pytest.mark.asyncio
async def test_bad_parameters(monkeypatch, query):
    async with make_client(monkeypatch) as client:
        res = await client.get("/api/traceroutes" + query)
        assert res.status == 400


@pytest.mark.asyncio
async def test_page_renders_window_and_channel(monkeypatch):
    async with make_client(monkeypatch) as client:
        res = await client.get("/tracelive/9999?channel=</script><b>x")
        assert res.status == 200
        text = await res.text()
        assert f"let minutes = {api.MAX_LIVE_TRACEROUTE_MINUTES};" in text
        assert "</script><b>x" not in text

        res = await client.get("/tracelive/0")
        assert "let minutes = 1;" in await res.text()

        res = await client.get("/tracelive?minutes=abc")
        assert "let minutes = 30;" in await res.text()
        assert 'let channel = "all";' in await res.text()
