import pytest
from services.create_order_handler import CreateOrderHandler


class TestCreateOrderHandler():

    def test_run_event(self):
        data = {
            'magic': 123456778,
            'ticker': 'XBV1 Index',
            'vol': 1,
            'order_type': 'LMT',
            'tif': 'DAY',
            'side': 'BUY',
        }

        handler = CreateOrderHandler(data)

        response = handler.run_event()

        assert 'request' in response.keys()