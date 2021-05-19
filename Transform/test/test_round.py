import unittest
from DataModel import DataFrame
from Transform import Round


class TestRound(unittest.TestCase):
    def setUp(self):
        self.df = DataFrame({'Cat': ["A", "A", "A", "B", "B", "B", "B", "C", "C", "C"],
                             'Date': ["2021-03-01", "2021-03-02", "2021-03-03",
                                      "2021-03-01", "2021-03-02", "2021-03-03", "2021-03-04",
                                      "2021-03-01", "2021-03-02", "2021-03-03"],
                             'Var1': [10.12, 14.2634, 13.9122, 22.74, 28.09, 23.1, 30, 6.061, 8.57, 9.532],
                             'Var2': [250.1823, 245.682109, 209.0111111111, 360.873421, 328.09534163, 359.07426, 372.07227173, 74, 78, 80],
                             'VarNone': [87.0912, 99.102, None, 120, 128.88, None, 99.64, None, None, None],
                             'VarMixed': [-3, 2, "Null", 0, None, "Null", 5, None, "Null", "Null"],
                             'VarTextNum': ["5", "8", "-1", "0", "7.4", "11.9", "-8.44", "5", -4.8, 9.2]})

    def test_round(self):
        arrondi = Round('Var1')
        result = arrondi.apply(self.df)
        self.assertEqual([10.11, 14.26, 13.91, 22.74, 28.09, 23.1, 30, 6.06, 8.57, 9.53], result[2])

    def test_round_with_precision(self):
        arrondi = Round('Var2', precision=3)
        result = arrondi.apply(self.df)
        print(result)
        self.assertEqual([250.182, 245.682, 209.011, 360.873, 328.095, 359.074, 372.072, 74, 78, 80], result[3])

    def test_round_with_None(self):
        arrondi = Round('VarNone')
        result = arrondi.apply(self.df)
        self.assertEqual([87.09, 99.10, None, 120, 128.88, None, 99.64, None, None, None], result[4])


if __name__ == '__main__':
    unittest.main()
