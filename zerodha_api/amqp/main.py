import json
from datetime import datetime
from typing import List

from celery import Celery
from redis import Redis

from zerodha_api.settings import KITE_TICK_DATA, KITE_SNAPSHOT_DATA
from zerodha_api.settings import get_configuration


def get_celery_app(
    redis_host: str = "127.0.0.1",
    redis_password: str = "",
    redis_port: int = 6379,
    redis_db: int = 0,
) -> Celery:
    """
    Creates and configures a Celery application using a Redis broker.
    This app includes a task to cache tick data in Redis.

    :param redis_host: The Redis server hostname or IP address. Defaults to "127.0.0.1".
    :param redis_password: The Redis server password. Defaults to an empty string.
    :param redis_port: The Redis server port. Defaults to 6379.
    :param redis_db: The Redis database number to connect to. Defaults to 0.
    :return: Celery: A configured Celery application instance with Redis as the broker.
    """

    config = get_configuration()
    redis_config = {
        "host": redis_host,
        "password": redis_password,
        "port": redis_port,
        "db": redis_db,
        "decode_responses": True,
    }

    redis_client = Redis(**redis_config)
    broker_url = f"redis://:{redis_password}@{redis_host}:{redis_port}/{redis_db}"

    app = Celery(
        "tasks",
        broker=broker_url,
    )
    app.conf.update(
        timezone=config["timezone"],
        enable_utc=True,
        broker_connection_retry_on_startup=True,
    )

    @app.task
    def cache_on_redis(ticks: List[dict]) -> None:
        """
        Caches tick data in Redis.

        :param ticks: A list of tick data dictionaries containing information such as 'instrument_token' and 'ltp'.
        :return:
        """
        for tick in ticks:
            instrument_token, ltp = tick["instrument_token"], tick["last_price"]
            redis_client.set(f"kt:ltp:{instrument_token}", ltp)
            redis_client.rpush(
                f"kt:{instrument_token}",
                json.dumps(
                    tick,
                    default=lambda o: o.__str__() if isinstance(o, datetime) else o,
                ),
            )
        tick_data = json.dumps(
            ticks, default=lambda o: o.__str__() if isinstance(o, datetime) else o
        )
        redis_client.set(KITE_SNAPSHOT_DATA, tick_data)
        redis_client.rpush(KITE_TICK_DATA, tick_data)

    return app
