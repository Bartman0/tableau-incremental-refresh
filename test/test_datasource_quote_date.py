from unittest import TestCase
import utils
from tableauhyperapi import Date

class Test(TestCase):
    def test_datasource_quote_date(self):
        self.assertEqual("#2021-01-15#", utils.datasource_quote_date(Date(2021,1,15)))
