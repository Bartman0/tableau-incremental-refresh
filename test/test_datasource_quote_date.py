from unittest import TestCase
import utils

class Test(TestCase):
    def test_datasource_quote_date(self):
        self.assertEqual(utils.datasource_quote_date("'2021-01-01'"), "'#2021-01-01#'")
