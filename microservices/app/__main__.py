from contextlib import asynccontextmanager
import os
from nats.js.api import ConsumerConfig, StreamConfig
import uvicorn
from typing import AsyncGenerator
from fastapi import FastAPI
from taskiq_redis import RedisAsyncResultBackend
import taskiq_fastapi
from app.broker import MultiServiceNatsBroker

SERVICE_NAME = os.environ.get("SERVICE_NAME", "user")
NATS_URLS = os.environ.get("NATS_URLS", "nats://localhost:4222").split(",")
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost")
STREAM_NAME = os.environ.get("STREAM_NAME", "org-stream")
SUBJECTS = os.environ.get("SUBJECTS", "org.>").split(",")
CONSUMER_NAME = os.environ.get("CONSUMER_NAME", "app")
FILTER_SUBJECTS = os.environ.get("FILTER_SUBJECTS", "org.app.>").split(",")

broker = MultiServiceNatsBroker(
    servers=NATS_URLS,
    stream_config=StreamConfig(
        name=STREAM_NAME,
        subjects=SUBJECTS,
    ),
    consumer_config=ConsumerConfig(
        name=CONSUMER_NAME,
        durable_name=CONSUMER_NAME,
        filter_subjects=FILTER_SUBJECTS,
    ),
).with_result_backend(RedisAsyncResultBackend(REDIS_URL, result_ex_time=10))


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncGenerator[None, None]:
    if not broker.is_worker_process:
        await broker.startup()

    yield

    if not broker.is_worker_process:
        await broker.shutdown()


app = FastAPI(title="app1", docs_url="/docs", lifespan=lifespan)
taskiq_fastapi.init(broker, app)


@broker.task(task_name="meme-org.app1.user.create")
async def user_create_task():
    print("User created")


@broker.task(task_name="meme-org.app2.meme.create")
async def meme_create_task():
    print("Meme created")


@app.post("/send-task")
async def meme():
    """Send task."""
    if SERVICE_NAME == "user":
        await user_create_task.kiq()
    else:
        await meme_create_task.kiq()
    return {}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, workers=1)
