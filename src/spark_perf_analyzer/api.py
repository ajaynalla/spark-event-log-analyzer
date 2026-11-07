"""Stateless FastAPI service. Raw uploads avoid multipart spooling before validation."""
from __future__ import annotations

import asyncio
import logging
import os
import time
import uuid
from pathlib import Path
from tempfile import TemporaryFile

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from starlette.concurrency import run_in_threadpool

from .web_analysis import LogError, MAX_BYTES, MAX_EVENTS, MAX_TASKS, analyze_stream

log = logging.getLogger("spark_analyzer")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
app = FastAPI(title="Spark Event Log Analyzer", version="1.0.0")
app.add_middleware(CORSMiddleware,
    allow_origins=[x.strip() for x in os.getenv("ALLOWED_ORIGINS", "http://localhost:5173,http://127.0.0.1:5173").split(",") if x.strip()],
    allow_methods=["GET", "POST"], allow_headers=["Content-Type"], expose_headers=["X-Request-ID"])
busy = False  # One Uvicorn process; never queue several memory-heavy analyses.
SAMPLE = Path(__file__).parent / "data" / "demo.jsonl"


@app.middleware("http")
async def request_log(request: Request, call_next):
    request_id = uuid.uuid4().hex[:12]
    started = time.monotonic()
    response = await call_next(request)
    response.headers["X-Request-ID"] = request_id
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Cache-Control"] = "no-store"
    # Do not log file contents, file names, query strings, or client identifiers.
    log.info("request=%s method=%s status=%s elapsed_ms=%.1f", request_id, request.method, response.status_code, (time.monotonic()-started)*1000)
    return response


@app.exception_handler(LogError)
async def invalid_log(request: Request, exc: LogError):
    return JSONResponse(status_code=exc.status, content={"error": {"code": exc.code, "message": str(exc)}})


@app.exception_handler(Exception)
async def unexpected(request: Request, exc: Exception):
    log.error("Unhandled analysis error type=%s", type(exc).__name__)
    return JSONResponse(status_code=500, content={"error": {"code": "internal_error", "message": "Analysis failed unexpectedly. Please try a smaller log."}})


@app.get("/health")
def health():
    return {"status": "ok", "version": "1.0.0", "commit": os.getenv("RENDER_GIT_COMMIT")}


@app.get("/api/config")
def config():
    return {"max_upload_bytes": MAX_BYTES, "max_events": MAX_EVENTS, "max_task_attempts": MAX_TASKS,
            "format": "uncompressed UTF-8 JSON Lines", "retention": "none"}


@app.get("/api/sample/download")
def download_sample():
    return FileResponse(SAMPLE, media_type="application/x-ndjson", filename="synthetic-spark-demo.jsonl")


@app.get("/api/sample")
async def sample():
    global busy
    if busy:
        raise LogError("Another log is being analyzed. Retry in a moment.", "busy", 429)
    busy = True
    try:
        with SAMPLE.open("rb") as handle:
            return await run_in_threadpool(analyze_stream, handle, sample=True)
    finally:
        busy = False


@app.post("/api/analyze")
async def upload(request: Request):
    global busy
    if busy:
        raise LogError("Another log is being analyzed. Retry in a moment.", "busy", 429)
    if request.headers.get("content-encoding", "identity") != "identity":
        raise LogError("Compressed uploads are unsupported. Decompress the event log first.", "unsupported_encoding", 415)
    if request.headers.get("content-type", "").split(";")[0] not in ("application/octet-stream", "application/x-ndjson", "application/json", "text/plain"):
        raise LogError("Send the log as a raw request body, not multipart/form-data.", "unsupported_type", 415)
    length = request.headers.get("content-length")
    if length and (not length.isdigit() or int(length) > MAX_BYTES):
        raise LogError("Event log exceeds the 25 MiB upload limit.", "too_large", 413)
    busy = True
    try:
        with TemporaryFile("w+b") as handle:
            size = 0
            # A slow upload cannot monopolize the single analysis slot indefinitely.
            async def receive_upload():
                nonlocal size
                async for chunk in request.stream():
                    size += len(chunk)
                    if size > MAX_BYTES:
                        raise LogError("Event log exceeds the 25 MiB upload limit.", "too_large", 413)
                    handle.write(chunk)
            try:
                await asyncio.wait_for(receive_upload(), timeout=60)
            except asyncio.TimeoutError as exc:
                raise LogError("Upload exceeded 60 seconds. Try a smaller log.", "upload_timeout", 408) from exc
            handle.seek(0)
            return await run_in_threadpool(analyze_stream, handle)
    finally:
        busy = False
