from unittest import TestCase
from DataModel import DataFrame
from Transform import Rename


class TestRename(TestCase):
    def setUp(self):
        self.df = DataFrame({'Cat': ["A", "A", "A", "B", "B", "B", "B", "C", "C", "C"],
                             'Date': ["2021-03-01", "2021-03-02", "2021-03-03",
                                      "2021-03-01", "2021-03-02", "2021-03-03", "2021-03-04",
                                      "2021-03-01", "2021-03-02", "2021-03-03"],
                             'Var1': [10, 14, 13, 22, 28, 23, 30, 6, 8, 9],
                             'Var2': [250, 245, 209, 360, 328, 359, 372, 74, 78, 80],
                             'VarMixed': [-3, 2, "Null", 0, None, "Null", 5, None, "Null", "Null"],
                             'VarTextNum': ["5", "8", "-1", "0", "7.4", "11.9", "-8.44", "5", -4.8, 9.2]})

    def test_rename(self):
        renames = Rename(Variable4='Var1', Experiment='VarMixed', Category='Cat', TextField='VarTextNum')
        result = renames.apply(self.df)
        self.assertEqual(['Category', 'Date', 'Variable4', 'Var2', 'Experiment', 'TextField'], result.vars)
