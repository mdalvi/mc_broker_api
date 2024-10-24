import time
from datetime import datetime, timedelta
from typing import Tuple, Optional

import numpy as np
import pandas as pd
from cryptography.fernet import Fernet
from kiteconnect import KiteConnect
from kiteconnect.exceptions import DataException, GeneralException, NetworkException
from redis import Redis
from retrying import retry

from zerodha_api.settings import KITE_HISTORICAL_API_RATE_LIMIT
from zerodha_api.settings import (
    get_fernet_secret,
    get_configuration,
)
from zerodha_api.settings import get_logger
from zerodha_api.utils.datetime import get_date_now

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

        # Extras
        self.KITE_HISTORICAL_DATA_REQUEST_INTERVAL_LIMIT = 60  # days

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

    # Function to check if the exception should trigger a retry
    @staticmethod
    def retry_if_exception(exception):
        return isinstance(
            exception, (DataException, NetworkException, GeneralException)
        )

    @retry(
        stop_max_attempt_number=5,
        wait_fixed=5000,
        retry_on_exception=retry_if_exception,
    )
    def _historical_data(self, *args, **kwargs):
        return self.kite.historical_data(*args, **kwargs)

    def _get_historical_step2(
        self,
        instrument_token: int,
        from_date: datetime.date,
        to_date: datetime.date,
        interval: str,
        continuous: bool,
        oi: bool,
    ) -> Tuple[pd.DataFrame, bool]:

        # Check Redis for the last API call timestamp
        recent_call_timestamp: str = self.redis_client.get(
            KITE_HISTORICAL_API_RATE_LIMIT
        )

        if recent_call_timestamp:
            # Calculate the time difference from the last API call
            elapsed_time = time.time() - float(recent_call_timestamp)
            # If the elapsed time is less than the required delay (1/3 second), wait
            # See https://kite.trade/docs/connect/v3/exceptions/ for more details
            if elapsed_time < 1 / 3:
                time.sleep(1 / 3 - elapsed_time)

        historical_df = pd.DataFrame(
            self._historical_data(
                instrument_token=instrument_token,
                from_date=from_date,
                to_date=to_date,
                interval=interval,
                continuous=continuous,
                oi=oi,
            )
        )

        # Update Redis with the current timestamp of the API call
        self.redis_client.set(KITE_HISTORICAL_API_RATE_LIMIT, time.time())

        if historical_df.shape[0] == 0:
            logger.info(
                f"kc:__get_historical_step2: no further historical data found for {instrument_token}"
            )
            return pd.DataFrame(), False

        # Renaming data columns from API results
        historical_df.rename(
            columns={
                "date": "record_datetime",
                "open": "open_price",
                "high": "high_price",
                "low": "low_price",
                "close": "close_price",
            },
            inplace=True,
        )
        historical_df["instrument_token"] = instrument_token
        historical_df["interval"] = interval
        logger.info(
            f"kc:__get_historical_step2: retrieved  further historical data found for {instrument_token}"
        )
        return historical_df, True

    def _get_historical_step1(
        self,
        instrument_token: int,
        from_date: datetime.date,
        to_date: datetime.date,
        interval: str,
        continuous: bool,
        oi: bool,
    ) -> pd.DataFrame:
        dt_diff = (to_date - from_date).days
        threshold_limit = self.KITE_HISTORICAL_DATA_REQUEST_INTERVAL_LIMIT
        historical_dfs = []
        if dt_diff > threshold_limit:
            logger.info(
                f"kc:__get_historical_step1: date difference of {dt_diff} > {threshold_limit} days, requesting data in chunks"
            )

            is_break = False
            while True:
                if (to_date - from_date).days > threshold_limit:
                    fd_new = to_date - timedelta(days=threshold_limit)
                    fd_new = datetime.strptime(
                        fd_new.strftime("%Y-%m-%d") + " 00:00:00", "%Y-%m-%d %H:%M:%S"
                    )
                else:
                    fd_new = to_date - timedelta(days=(to_date - from_date).days)
                    fd_new = datetime.strptime(
                        fd_new.strftime("%Y-%m-%d") + " 00:00:00", "%Y-%m-%d %H:%M:%S"
                    )
                    is_break = True

                logger.info(
                    f"kc:__get_historical_step1: getting historical data from {fd_new} to {to_date}"
                )
                historical_df, has_data = self._get_historical_step2(
                    instrument_token, fd_new, to_date, interval, continuous, oi
                )

                if has_data:
                    historical_dfs.append(historical_df)

                if is_break:
                    break

                if not has_data:
                    break

                to_date = fd_new
                to_date = datetime.strptime(
                    to_date.strftime("%Y-%m-%d") + " 23:59:59", "%Y-%m-%d %H:%M:%S"
                )
        else:
            logger.info(
                f"kc:__get_historical_step1: getting historical data from {from_date} to {to_date}"
            )
            historical_df, _ = self._get_historical_step2(
                instrument_token, from_date, to_date, interval, continuous, oi
            )
            historical_dfs.append(historical_df)

        hist_final_df = pd.concat(historical_dfs)
        hist_final_df["record_datetime"] = pd.to_datetime(
            hist_final_df["record_datetime"]
        )
        hist_final_df.drop_duplicates(inplace=True)
        hist_final_df.reset_index(drop=True, inplace=True)
        hist_final_df["record_date"] = hist_final_df["record_datetime"].dt.date
        hist_final_df["record_time"] = hist_final_df["record_datetime"].dt.time
        hist_final_df["record_day"] = hist_final_df["record_datetime"].dt.day
        hist_final_df["record_month"] = hist_final_df["record_datetime"].dt.month
        hist_final_df["record_year"] = hist_final_df["record_datetime"].dt.year
        hist_final_df["record_weekday"] = hist_final_df["record_datetime"].dt.weekday
        hist_final_df["record_week_of_year"] = (
            hist_final_df["record_datetime"].dt.isocalendar().week
        )
        return hist_final_df

    def historical_data(
        self,
        instrument_token: int,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        interval: str = "day",
        continuous: bool = False,
        oi: bool = True,
    ) -> pd.DataFrame:
        """
        Fetches historical market data for a given instrument within a specified date range and interval.
        Extension of https://kite.trade/docs/pykiteconnect/v4/#kiteconnect.KiteConnect.historical_data

        :param instrument_token: The instrument token for which to fetch data.
        :param from_date:  The start date for the data retrieval in 'YYYY-MM-DD' format.
                If the value is 'None' then by default last 5 calendar days data is fetched
        :param to_date: The end date for the data retrieval in 'YYYY-MM-DD' format.
                If the value is 'None" then by default the to_date is yesterday's date.
        :param interval: The time interval for the data (e.g., 'day', '15minute'. '5minute'). Defaults to 'day'.
        :param continuous: Whether to fetch continuous data for futures. Defaults to False.
        :param oi: Whether to include open interest data. Defaults to True.
        :return: pd.DataFrame: A DataFrame containing the historical market data.
        """
        if from_date is None:
            from_date = (
                get_date_now(self.config["timezone"]) - timedelta(days=5)
            ).strftime("%Y-%m-%d")
            from_date = datetime.strptime(from_date + " 00:00:00", "%Y-%m-%d %H:%M:%S")
        else:
            from_date = datetime.strptime(from_date + " 00:00:00", "%Y-%m-%d %H:%M:%S")

        if to_date is None:
            to_date = (
                get_date_now(self.config["timezone"]) - timedelta(days=1)
            ).strftime("%Y-%m-%d")
            to_date = datetime.strptime(to_date + " 23:59:59", "%Y-%m-%d %H:%M:%S")
        else:
            to_date = datetime.strptime(to_date + " 23:59:59", "%Y-%m-%d %H:%M:%S")

        assert instrument_token is not None, "Attribute `instrument` cannot be None"
        assert interval is not None, "Attribute `interval` cannot be None"

        logger.info(
            f"kc:historical_data: starting historical data download for instrument_token {instrument_token} with interval '{interval}'"
        )
        hist_final_df = self._get_historical_step1(
            instrument_token, from_date, to_date, interval, continuous, oi
        )
        nb_records = hist_final_df.shape[0]
        logger.info(
            f"kc:historical_data: historical data download for instrument_token {instrument_token} with interval '{interval}' completed with #{nb_records} records"
        )
        return hist_final_df

    @retry(
        stop_max_attempt_number=5,
        wait_fixed=5000,
        retry_on_exception=retry_if_exception,
    )
    def _instruments(self, exchange: Optional[str] = None):
        return self.kite.instruments(exchange=exchange)

    def instruments(self, exchange: Optional[str] = None) -> pd.DataFrame:
        """
        Fetch instrument data from Kite API and return as a pandas DataFrame.
        Extension of https://kite.trade/docs/pykiteconnect/v4/#kiteconnect.KiteConnect.instruments

        :param exchange: An optional exchange identifier for which to fetch data;
            e.g. ("NSE", "BSE", "NFO", "CDS", "BFO", "MCX", "BCD")
        :return: pd.DataFrame: Processed instrument data.
        """
        logger.info("kc:instruments: starting instruments data download from kite api")
        instruments_df = pd.DataFrame(self._instruments(exchange=exchange))
        instruments_df.reset_index(drop=True, inplace=True)
        instruments_df["name"] = instruments_df["name"].replace("", np.nan)
        instruments_df["expiry"] = instruments_df["expiry"].replace("", np.nan)

        nb_ins = instruments_df.shape[0]
        logger.info(
            f"kc:instruments: instruments data download completed with #{nb_ins} records"
        )
        return instruments_df

    def positions(self, *args, **kwargs):
        """
        https://kite.trade/docs/pykiteconnect/v4/#kiteconnect.KiteConnect.positions
        :param args:
        :param kwargs:
        :return:
        """
        return self.kite.positions()
