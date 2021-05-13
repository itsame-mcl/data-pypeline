from unittest import TestCase
from DataModel import DataFrame
from Transform import Ungroup


class TestUngroup(TestCase):
    def setUp(self):
        self.df = DataFrame({'Cat': ["A", "A", "A", "B", "B", "B", "B", "C", "C", "C"],
                             'Date': ["2021-03-01", "2021-03-02", "2021-03-03",
                                      "2021-03-01", "2021-03-02", "2021-03-03", "2021-03-04",
                                      "2021-03-01", "2021-03-02", "2021-03-03"],
                             'Var1': [10, 14, 13, 22, 28, 23, 30, 6, 8, 9],
                             'Var2': [250, 245, 209, 360, 328, 359, 372, 74, 78, 80]})
        self.df.add_group('Cat')

    def test_ungroup(self):
        self.assertEqual([0, 0, 0, 0, 0, 0, 0, 0, 0, 0], Ungroup().apply(self.df).groups)
