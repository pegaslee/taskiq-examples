from logging import getLogger
from typing import (
    Any,
    AsyncGenerator,
    Union,
)

from nats.aio.client import Client
from nats.errors import TimeoutError
from nats.js.api import ConsumerConfig, StreamConfig
from nats.js.errors import NotFoundError
from taskiq import AckableMessage, AsyncBroker, BrokerMessage


logger = getLogger("taskiq.nats")


class MultiServiceNatsBroker(AsyncBroker):
    def __init__(
        self,
        servers: list[str] | None = None,
        connect_args: dict[str, Any] | None = None,
        stream_config: StreamConfig | None = None,
        consumer_config: ConsumerConfig | None = None,
        fetch_timeout: float | None = None,
        fetch_batch: int = 1,
    ) -> None:
        super().__init__()
        self.client: Client = Client()
        self.servers = servers or ["nats://localhost:4222"]
        self.js = self.client.jetstream()
        self.connect_args = connect_args or {}
        self.fetch_timeout = fetch_timeout
        self.fetch_batch = fetch_batch

        self.stream_config = stream_config or StreamConfig()
        self.stream_config.name = self.stream_config.name or "taskiq"

        self.consumer_config = consumer_config or ConsumerConfig()
        self.consumer_config.name = self.consumer_config.name or "taskiq"

    async def _declare_stream(self) -> None:
        """Function to declare a JetStream's stream."""
        try:
            await self.js.stream_info(self.stream_config.name or "taskiq")
        except NotFoundError:
            await self.js.add_stream(self.stream_config)
        await self.js.update_stream(self.stream_config)

    async def startup(self) -> None:
        """
        Start the broker.

        This function will declare a JetStream's stream and consumer.
        Stream is used to store information about all topics and consumers
        are used to store information about received and acknowledged messages.
        """
        await super().startup()
        await self.client.connect(self.servers, **self.connect_args)
        await self._declare_stream()
        if self.is_worker_process:
            await self.js.add_consumer(
                self.stream_config.name,  # type: ignore
                self.consumer_config,
            )

    async def kick(self, message: BrokerMessage) -> None:
        """Publish a message to a topic."""
        await self.client.publish(message.task_name, message.message)

    async def listen(self) -> AsyncGenerator[Union[bytes, AckableMessage], None]:
        """Listen for messages from a stream."""
        psub = await self.js.pull_subscribe_bind(
            self.consumer_config.name,
            self.stream_config.name,
        )
        while True:
            try:
                msgs = await psub.fetch(
                    timeout=self.fetch_timeout,
                    batch=self.fetch_batch,
                )
            except TimeoutError:
                logger.debug("Timeout while fetching messages.")
                continue
            for msg in msgs:
                yield AckableMessage(data=msg.data, ack=msg.ack)
