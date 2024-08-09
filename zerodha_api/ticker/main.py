from typing import List

from celery import Celery
from cryptography.fernet import Fernet
from kiteconnect import KiteTicker
from redis import Redis

from zerodha_api.settings import (
    get_fernet_secret,
    get_configuration,
    KITE_SUBSCRIPTIONS,
    KITE_TICKER_STOP,
    KITE_TICKER_IS_LIVE,
    KITE_SUBSCRIPTIONS_UPDATE,
    KITE_ORDER_UPDATE,
)
from zerodha_api.settings import get_logger

logger = get_logger()


class Ticker:
    def __init__(
        self,
        token: str,
        redis_host: str = "127.0.0.1",
        redis_password: str = "",
        redis_port: int = 6379,
        redis_db: int = 0,
    ):
        """
        A class that initializes and manages a KiteTicker connection, Redis client,
        and Celery application for real-time market data processing.

        :param token: Encrypted access token required for the Kite API.
        :param redis_host: The Redis server hostname or IP address. Defaults to "127.0.0.1".
        :param redis_password: The Redis server password. Defaults to an empty string.
        :param redis_port: The Redis server port. Defaults to 6379.
        :param redis_db: The Redis database number to connect to. Defaults to 0.
        """
        self.config = get_configuration()

        # The ticker is subscribed to NIFTY 50, NIFTY BANK and INDIA VIX by default
        self.subscriptions = [256265, 260105, 264969]

        debug = bool(self.config["debug"])
        api_key = self.config["kite_api_key"]
        cipher_suite = Fernet(get_fernet_secret())
        access_token = cipher_suite.decrypt(token).decode("utf-8")
        self.kite = KiteTicker(api_key=api_key, access_token=access_token, debug=debug)

        redis_config = {
            "host": redis_host,
            "password": redis_password,
            "port": redis_port,
            "db": redis_db,
            "decode_responses": True,
        }
        self.redis_client = Redis(**redis_config)

        broker_url = f"redis://:{redis_password}@{redis_host}:{redis_port}/{redis_db}"
        self.zamqp = Celery(
            "tasks",
            broker=broker_url,
        )
        self.zamqp.conf.update(
            timezone=self.config["timezone"],
            enable_utc=True,
            broker_connection_retry_on_startup=True,
        )

    def _set_instruments(self, instruments):
        """
        Set the list of instruments in Redis.
        :param instruments: List of integer instrument IDs
        """
        # Use rpush to add all instruments to the list
        self.redis_client.rpush(KITE_SUBSCRIPTIONS, *instruments)

    def _get_instruments(self) -> List[int]:
        """
        Retrieve the list of instruments from Redis.
        :return: List of integer instrument IDs
        """
        # Use lrange to get all elements of the list
        instruments = self.redis_client.lrange(KITE_SUBSCRIPTIONS, 0, -1)

        # Convert bytes to integers
        return [int(instrument) for instrument in instruments]

    def run(self):
        self.kite.on_open = self.on_open
        self.kite.on_connect = self.on_connect
        self.kite.on_ticks = self.on_ticks
        self.kite.on_close = self.on_close
        self.kite.on_reconnect = self.on_reconnect
        self.kite.on_error = self.on_error
        self.kite.on_order_update = self.on_order_update
        self.kite.connect()

    @staticmethod
    def on_open(ws):
        """
        Called when the initial WebSocket opening handshake was completed.
        :param ws: web socket
        :return: None
        """
        logger.info("kt:on_open: completed")

    def on_connect(self, ws, response):
        """
        Triggered when connection is established successfully.
        :param ws: web socket
        :param response: Response received from server on successful connection.
        :return: None
        """
        subscriptions = self.subscriptions + self._get_instruments()
        ws.subscribe(subscriptions)
        ws.set_mode(ws.MODE_FULL, subscriptions)

        nb_subscriptions = subscriptions.__len__()
        logger.info(f"kt:on_connect: subscribed for #{nb_subscriptions} instruments")
        logger.info(f"kt:on_connect: completed")

    def on_ticks(self, ws, ticks):
        """
        Triggered when ticks are received.
        :param ws: web socket
        :param ticks: List of tick object. Check below for sample structure.
        :return: None
        """
        # Push the ticks data to the Celery task queue
        self.zamqp.send_task("zerodha_api.amqp.main.cache_on_redis", args=[ticks])

        if self.redis_client.get(KITE_TICKER_STOP) == "1":
            self.kite.close()
        else:
            self.redis_client.setex(KITE_TICKER_IS_LIVE, 3, 1)

            # Re-subscribe on flagged
            if self.redis_client.get(KITE_SUBSCRIPTIONS_UPDATE) == "1":
                subscriptions = self.subscriptions + self._get_instruments()

                ws.subscribe(subscriptions)
                ws.set_mode(ws.MODE_FULL, subscriptions)
                self.redis_client.set(KITE_SUBSCRIPTIONS_UPDATE, 0)
                nb_subscriptions = subscriptions.__len__()
                logger.info(
                    f"kt:on_ticks: resubscribed to #{nb_subscriptions} instruments"
                )
        logger.info("kt:on_ticks: completed")

    @staticmethod
    def on_reconnect(ws, attempts_count):
        """
        Triggered when auto reconnection is attempted.
        :param ws: web socket
        :param attempts_count: Current reconnect attempt number.
        :return: None
        """
        logger.warning(f"kt:on_reconnect: attempt #{attempts_count} completed")

    @staticmethod
    def on_noreconnect(ws):
        """
        Triggered when number of auto reconnection attempts exceeds reconnect_tries.
        :param ws: web socket
        :return: None
        """
        logger.warning("kt:on_noreconnect completed")

    @staticmethod
    def on_close(ws, code, reason):
        """
        Triggered when connection is closed.
        :param ws: web socket
        :param code: WebSocket standard close event code (https://developer.mozilla.org/en-US/docs/Web/API/CloseEvent)
        :param reason: DOMString indicating the reason the server closed the connection
        :return: None
        """
        logger.info(f"kt:on_close: code [{code}] reason [{reason}] completed")

        # Sending stop signal to web-socket
        # ws.stop()

    @staticmethod
    def on_error(ws, code, reason):
        """
        Triggered when connection is closed with an error.
        :param ws: web socket
        :param code: WebSocket standard close event code (https://developer.mozilla.org/en-US/docs/Web/API/CloseEvent)
        :param reason: DOMString indicating the reason the server closed the connection
        :return: None
        """
        logger.warning(f"kt:on_error: code [{code}] reason [{reason}] completed")

    @staticmethod
    def on_message(ws, payload, is_binary):
        """
        Triggered when message is received from the server.
        :param ws: web socket
        :param payload: Raw response from the server (either text or binary).
        :param is_binary: Bool to check if response is binary type.
        :return: None
        """
        if is_binary:
            payload = payload.decode("utf-8")
        logger.info(f"kt:on_message is_binary [{is_binary}], payload [{payload}]")

    def on_order_update(self, ws, data):
        """
        Triggered when there is an order update for the connected user.
        :param ws: web socket
        :param data: data
        :return: None
        """
        self.redis_client.set(KITE_ORDER_UPDATE, 1)
        logger.info(f"kt:on_order_update: completed")
