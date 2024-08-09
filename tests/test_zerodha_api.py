"""
Test case generated using Claude Sonnet 3.5
"""

import json
from unittest.mock import patch

import pytest

from zerodha_api import ZAMQp
from zerodha_api.settings import KITE_SNAPSHOT_DATA, KITE_TICK_DATA


@pytest.fixture
def redis_config():
    return {
        "redis_host": "127.0.0.1",
        "redis_password": "",
        "redis_port": 6379,
        "redis_db": 0,
    }


@pytest.fixture
def mock_redis():
    with patch("zerodha_api.amqp.main.Redis") as mock:
        yield mock.return_value


@pytest.fixture
def mock_get_configuration():
    with patch("zerodha_api.amqp.main.get_configuration") as mock:
        mock.return_value = {"timezone": "UTC"}
        yield mock


def test_create_celery_app(redis_config, mock_redis, mock_get_configuration):
    app = ZAMQp(**redis_config)

    assert app.main == "tasks"
    assert app.conf.timezone == "UTC"
    assert app.conf.enable_utc is True
    assert app.conf.broker_connection_retry_on_startup is True

    expected_broker_url = f"redis://:{redis_config['redis_password']}@{redis_config['redis_host']}:{redis_config['redis_port']}/{redis_config['redis_db']}"
    assert app.conf.broker_url == expected_broker_url


def test_cache_on_redis_task(redis_config, mock_redis, mock_get_configuration):
    app = ZAMQp(**redis_config)
    # Get the task by name from the app
    task = app.tasks["zerodha_api.amqp.main.cache_on_redis"]

    test_ticks = [
        {"instrument_token": "123", "last_price": 100.5},
        {"instrument_token": "456", "last_price": 200.75},
    ]

    task.apply(args=(test_ticks,))

    # Check if Redis set and rpush were called with correct arguments
    for tick in test_ticks:
        mock_redis.set.assert_any_call(
            f"kt:ltp:{tick['instrument_token']}", tick["last_price"]
        )
        mock_redis.rpush.assert_any_call(
            f"kt:{tick['instrument_token']}", json.dumps(tick)
        )

    # Check if KITE_SNAPSHOT_DATA and KITE_TICK_DATA were set
    mock_redis.set.assert_any_call(KITE_SNAPSHOT_DATA, json.dumps(test_ticks))
    mock_redis.rpush.assert_any_call(KITE_TICK_DATA, json.dumps(test_ticks))


if __name__ == "__main__":
    pytest.main()
