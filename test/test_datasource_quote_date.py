from unittest import TestCase
import utils

class Test(TestCase):
    def test_datasource_quote_date(self):
        self.assertEqual("#2021-01-01#", utils.datasource_quote_date("'2021-01-01'"))
