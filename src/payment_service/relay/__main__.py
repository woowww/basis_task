import asyncio
import signal

from payment_service.config import get_settings
from payment_service.infra.broker.provider import create_broker
from payment_service.infra.broker.topology import Topology
from payment_service.ioc import create_container
from payment_service.logging_config import configure_logging
from payment_service.relay.runner import run_relay


async def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    broker = create_broker(settings)
    container = create_container(settings, broker=broker)
    await broker.connect()
    try:
        await (await container.get(Topology)).declare_exchange(broker)
        await run_relay(container, settings, stop)
    finally:
        await container.close()
        await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
