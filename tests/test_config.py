from unittest import TestCase
from config import secrets_endpoint, server_listing_endpoint, stocks_endpoint


class ConfigTests(TestCase):

    # Test that GET requests to API endpoint(s) always return a 200 response
    def test_api_endpoints(self):

        self.assertEqual(secrets_endpoint.status_code, 200)
        self.assertEqual(server_listing_endpoint.status_code, 200)
        self.assertEqual(stocks_endpoint.status_code, 200)