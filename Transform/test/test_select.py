from unittest import TestCase
from DataModel import DataFrame
from Transform import Select


class TestSelect(TestCase):
    def setUp(self):
        self.df = DataFrame({'Cat': ["A", "A", "A", "B", "B", "B", "B", "C", "C", "C"],
                             'Date': ["2021-03-01", "2021-03-02", "2021-03-03",
                                      "2021-03-01", "2021-03-02", "2021-03-03", "2021-03-04",
                                      "2021-03-01", "2021-03-02", "2021-03-03"],
                             'Var1': [10, 14, 13, 22, 28, 23, 30, 6, 8, 9],
                             'Var2': [250, 245, 209, 360, 328, 359, 372, 74, 78, 80],
                             'VarNone': [87, 99, None, 120, 128, None, 99, None, None, None],
                             'VarMixed': [-3, 2, "Null", 0, None, "Null", 5, None, "Null", "Null"],
                             'VarTextNum': ["5", "8", "-1", "0", "7.4", "11.9", "-8.44", "5", -4.8, 9.2]})

    def test_one_var(self):
        one_var = Select('Var1')
        result = one_var.apply(self.df)
        self.assertEqual([10, 14, 13, 22, 28, 23, 30, 6, 8, 9], result[0])

    def test_multiple_vars(self):
        multiple_vars = Select('Var2', 'VarMixed', 'Date', 'Var1', 'VarNone')
        result = multiple_vars.apply(self.df)
        self.assertEqual([209, "Null", "2021-03-03", 13, None], result[None, 2])
