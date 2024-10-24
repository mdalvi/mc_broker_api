# Zerodha API
[![codecov](https://codecov.io/github/mdalvi/zerodha_api/graph/badge.svg?token=ZYRWQBZG7J)](https://codecov.io/github/mdalvi/zerodha_api)

This is a data aggregation wrapper for Zerodha's Kite Connect API that simplifies market data handling for machine learning and data analysis.

## Features

```python
# TODO
```
## Installation

```python
# TODO
```

## Prerequisites

```python
# TODO
```

## Configuration

```python
# TODO
```

## Usage

```python
# TODO
```

## Key Components

- `Connect`: Main class for interacting with Kite API
- `Ticker`: WebSocket implementation for real-time market data
- `Redis`: Caching layer for market data and state management
- `Celery`: Asynchronous task processing for data handling

## Development

```bash
# Install development dependencies
poetry install ../zerodha_api/

# Run tests
pytest

# Generate coverage report
pytest --cov=./ --cov-report=xml
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- [Zerodha Kite Connect API](https://kite.trade/docs/connect/v3/)
- [KiteConnect Python Client](https://kite.trade/docs/pykiteconnect/v4/)