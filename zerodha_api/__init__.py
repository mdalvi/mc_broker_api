import logging

from zerodha_api.amqp.main import get_celery_app as ZAMQp
from zerodha_api.connect.main import Connect as ZerodhaConnect
from zerodha_api.settings import (
    configure_logger,
    set_configuration,
    get_configuration,
    set_fernet_secret,
)
from zerodha_api.ticker.main import Ticker as ZerodhaTicker

# Create a null handler to avoid "No handler found" warnings.
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

set_configuration()
set_fernet_secret()
