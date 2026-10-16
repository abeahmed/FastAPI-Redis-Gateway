import asyncio
from os import path
import time
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse
import httpx
import uvicorn
import redis.asyncio as aioredis

upstream_app = FastAPI()

async def mock_upstream_stream():
    tokens = ["Gateway", "is", "forwarding", "perfectly"]
    for token in tokens:
        yield f"data: {token}\n\n"
        await asyncio.sleep(0.3)

@upstream_app.get("/api/data")
async def mock_upstream():
    return StreamingResponse(
        mock_upstream_stream(),
        media_type = "text/event-stream"
    )

TOKEN_BUCKET_LUA = """
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])

local data = redis.call("HMGET", key, "tokens", "last_updated")
local tokens = tonumber(data[1])
local last_updated = tonumber(data[2])

if tokens == nil then
    tokens = capacity
    last_updated = now
else
    local delta = math.max(0, now - last_updated)
    tokens = math.min(capacity, tokens + (delta * refill_rate))
    last_updated = now
end

if tokens >= requested then
    tokens = tokens - requested
    redis.call("HMSET", key, "tokens", tokens, "last_updated", last_updated)
    redis.call("EXPIRE", key, math.ceil(capacity / refill_rate))
    return 1
else
    redis.call("HMSET", key, "tokens", tokens, "last_updated", last_updated)
    return 0
end
"""

gateway_app = FastAPI()

http_client = httpx.AsyncClient(base_url="http://127.0.0.1:8001")
redis_client: aioredis.Redis = None
rate_limit_script = None

MAX_CAPACITY = 5
REFILL_RATE = 0.5

@asynccontextmanager
async def gateway_lifespan(app: FastAPI):
    global http_client, redis_client, rate_limit_script

    http_client = httpx.AsyncClient(base_url="http://127.0.0.1:8001")
    redis_client = aioredis.from_url("redis://localhost:6379", decode_responses=True)

    rate_limit_script = redis_client.register_script(TOKEN_BUCKET_LUA)

    yield

    await http_client.aclose()
    await redis_client.aclose()

gateway_app = FastAPI(title="API Gateway", lifespan=gateway_lifespan)

async def check_rate_limt_redis(client_ip: str) -> bool:
    key = f"rate_limit:{client_ip}"
    now = time.time()

    allowed = await rate_limit_script(
        keys=[key],
        args = [MAX_CAPACITY, REFILL_RATE, now, 1]
    )
    return bool(allowed)

@gateway_app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def reverse_proxy(request: Request, path: str):
    client_ip = request.client.host

    is_allowed = await check_rate_limt_redis(client_ip)
    if not is_allowed:
        raise HTTPException(status_code=429, detail=f"Rate limit exceeded. Try again later")
    
    print(f"[Gateway] Intercepted request for path: /{path}. Forwarding...")

    url = httpx.URL(path=f"/{path}", query=request.url.query.encode("utf-8"))

    upstream_req = http_client.build_request(
        method=request.method,
        url=url,
        headers=request.headers.raw,
        content=await request.body()
    )

    try:
        upstream_res = await http_client.send(upstream_req, stream=True)
    except httpx.RequestError as exc:
        raise HTTPException(status_code=502, detail=f"Upstream unreachable: {exc}")

    if upstream_res.status_code == 404:
        await upstream_res.aclose()
        raise HTTPException(status_code=404, detail="The requested resource was not found.")

    return StreamingResponse(
        upstream_res.aiter_raw(),
        status_code=upstream_res.status_code,
        headers=dict(upstream_res.headers),
    )

async def main():
    config_gateway = uvicorn.Config(gateway_app, host="127.0.0.1", port=8000, log_level="info")
    config_upstream = uvicorn.Config(upstream_app, host = "127.0.0.1", port=8001, log_level="info")

    server_gateway = uvicorn.Server(config_gateway)
    server_upstream = uvicorn.Server(config_upstream)

    await asyncio.gather(
        server_gateway.serve(),
        server_upstream.serve()
    )

if __name__ == "__main__":
    asyncio.run(main())
