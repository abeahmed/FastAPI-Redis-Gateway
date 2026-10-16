import asyncio
from os import path
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse
import httpx
import uvicorn

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

gateway_app = FastAPI()

http_client = httpx.AsyncClient(base_url="http://127.0.0.1:8001")

@gateway_app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def reverse_proxy(request: Request, path: str):
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
