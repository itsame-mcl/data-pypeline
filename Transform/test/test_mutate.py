import unittest
from DataModel import DataFrame
from Transform import Mutate


class TestMutate(unittest.TestCase):
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

    def test_compute_var(self):
        compute_var = Mutate(Var3="Var1+Var2")
        result = compute_var.apply(self.df)
        self.assertEqual([260, 259, 222, 382, 356, 382, 402, 80, 86, 89], result['Var3'])

    def test_vars_with_lag(self):
        vars_with_lag = Mutate(Var4="Var1-lag_Var1")
        result = vars_with_lag.apply(self.df)
        self.assertEqual([None, 4, -1, 9, 6, -5, 7, -24, 2, 1], result['Var4'])

    def test_leads_with_groups(self):
        self.df.add_group('Cat')
        leads_with_goups = Mutate(Var5='Var2+lead_Var2')
        result = leads_with_goups.apply(self.df)
        self.assertEqual([495, 454, None, 688, 687, 731, None, 152, 158, None], result['Var5'])


if __name__ == '__main__':
    unittest.main()
