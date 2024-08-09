from cryptography.fernet import Fernet
from kiteconnect import KiteConnect
from redis import Redis

from zerodha_api.settings import (
    get_fernet_secret,
    get_configuration,
)
from zerodha_api.settings import get_logger

logger = get_logger()


class Connect:
    def __init__(
        self,
        token: str,
        redis_host: str = "127.0.0.1",
        redis_password: str = "",
        redis_port: int = 6379,
        redis_db: int = 0,
    ):
        """
        A class that initializes and manages a KiteConnect connection and Redis client for market data processing.

        :param token: Encrypted access token required for the Kite API.
        :param redis_host: The Redis server hostname or IP address. Defaults to "127.0.0.1".
        :param redis_password: The Redis server password. Defaults to an empty string.
        :param redis_port: The Redis server port. Defaults to 6379.
        :param redis_db: The Redis database number to connect to. Defaults to 0.
        """
        self.config = get_configuration()

        debug = bool(self.config["debug"])
        api_key = self.config["kite_api_key"]
        cipher_suite = Fernet(get_fernet_secret())
        access_token = cipher_suite.decrypt(token).decode("utf-8")
        self.kite = KiteConnect(api_key=api_key, access_token=access_token, debug=debug)
        redis_config = {
            "host": redis_host,
            "password": redis_password,
            "port": redis_port,
            "db": redis_db,
            "decode_responses": True,
        }
        self.redis_client = Redis(**redis_config)

        # Products
        self.PRODUCT_MIS = self.kite.PRODUCT_MIS
        self.PRODUCT_CNC = self.kite.PRODUCT_CNC
        self.PRODUCT_NRML = self.kite.PRODUCT_NRML
        self.PRODUCT_CO = self.kite.PRODUCT_CO

        # Order types
        self.ORDER_TYPE_MARKET = self.kite.ORDER_TYPE_MARKET
        self.ORDER_TYPE_LIMIT = self.kite.ORDER_TYPE_LIMIT
        self.ORDER_TYPE_SLM = self.kite.ORDER_TYPE_SLM
        self.ORDER_TYPE_SL = self.kite.ORDER_TYPE_SL

        # Varieties
        self.VARIETY_REGULAR = self.kite.VARIETY_REGULAR
        self.VARIETY_CO = self.kite.VARIETY_CO
        self.VARIETY_AMO = self.kite.VARIETY_AMO
        self.VARIETY_ICEBERG = self.kite.VARIETY_ICEBERG
        self.VARIETY_AUCTION = self.kite.VARIETY_AUCTION

        # Transaction type
        self.TRANSACTION_TYPE_BUY = self.kite.TRANSACTION_TYPE_BUY
        self.TRANSACTION_TYPE_SELL = self.kite.TRANSACTION_TYPE_SELL

        # Validity
        self.VALIDITY_DAY = self.kite.VALIDITY_DAY
        self.VALIDITY_IOC = self.kite.VALIDITY_IOC
        self.VALIDITY_TTL = self.kite.VALIDITY_TTL

        # Position Type
        self.POSITION_TYPE_DAY = self.kite.POSITION_TYPE_DAY
        self.POSITION_TYPE_OVERNIGHT = self.kite.POSITION_TYPE_OVERNIGHT

        # Exchanges
        self.EXCHANGE_NSE = self.kite.EXCHANGE_NSE
        self.EXCHANGE_BSE = self.kite.EXCHANGE_BSE
        self.EXCHANGE_NFO = self.kite.EXCHANGE_NFO
        self.EXCHANGE_CDS = self.kite.EXCHANGE_CDS
        self.EXCHANGE_BFO = self.kite.EXCHANGE_BFO
        self.EXCHANGE_MCX = self.kite.EXCHANGE_MCX
        self.EXCHANGE_BCD = self.kite.EXCHANGE_BCD

        # Margins segments
        self.MARGIN_EQUITY = self.kite.MARGIN_EQUITY
        self.MARGIN_COMMODITY = self.kite.MARGIN_COMMODITY

        # Status constants
        self.STATUS_COMPLETE = self.kite.STATUS_COMPLETE
        self.STATUS_REJECTED = self.kite.STATUS_REJECTED
        self.STATUS_CANCELLED = self.kite.STATUS_CANCELLED

        # GTT order type
        self.GTT_TYPE_OCO = self.kite.GTT_TYPE_OCO
        self.GTT_TYPE_SINGLE = self.kite.GTT_TYPE_SINGLE

        # GTT order status
        self.GTT_STATUS_ACTIVE = self.kite.GTT_STATUS_ACTIVE
        self.GTT_STATUS_TRIGGERED = self.kite.GTT_STATUS_TRIGGERED
        self.GTT_STATUS_DISABLED = self.kite.GTT_STATUS_DISABLED
        self.GTT_STATUS_EXPIRED = self.kite.GTT_STATUS_EXPIRED
        self.GTT_STATUS_CANCELLED = self.kite.GTT_STATUS_CANCELLED
        self.GTT_STATUS_REJECTED = self.kite.GTT_STATUS_REJECTED
        self.GTT_STATUS_DELETED = self.kite.GTT_STATUS_DELETED

    def historical_data(self, *args, **kwargs):
        """
        https://kite.trade/docs/pykiteconnect/v4/#kiteconnect.KiteConnect.historical_data
        :param args:
        :param kwargs:
        :return:
        """
        return self.kite.historical_data(*args, **kwargs)

    def instruments(self, *args, **kwargs):
        """
        https://kite.trade/docs/pykiteconnect/v4/#kiteconnect.KiteConnect.instruments
        :param args:
        :param kwargs:
        :return:
        """
        return self.kite.instruments(*args, **kwargs)

    def positions(self, *args, **kwargs):
        """
        https://kite.trade/docs/pykiteconnect/v4/#kiteconnect.KiteConnect.positions
        :param args:
        :param kwargs:
        :return:
        """
        return self.kite.positions()
