from unittest import TestCase
from DataModel import DataFrame
from Transform import Normalize


class TestNormalize(TestCase):
    def setUp(self):
        self.df = DataFrame({'Cat': ["A", "A", "A", "B", "B", "B", "B", "C", "C", "C"],
                             'Date': ["2021-03-01", "2021-03-02", "2021-03-03",
                                      "2021-03-01", "2021-03-02", "2021-03-03", "2021-03-04",
                                      "2021-03-01", "2021-03-02", "2021-03-03"],
                             'Var1': [10, 14, 13, 22, 28, 23, 30, 6, 8, 9],
                             'Var2': [250, 245, 209, 360, 328, 359, 372, 74, 78, 80]})

    def test_center(self):
        center = Normalize('Var1', 'Var2', reduce=False)
        result = center.apply(self.df)
        self.assertAlmostEqual(11.7, result['Var1_Std', 4])
        self.assertAlmostEqual(-26.5, result['Var2_Std', 2])

    def test_reduce(self):
        reduce = Normalize('Var1', 'Var2', center=False)
        result = reduce.apply(self.df)
        self.assertAlmostEqual(0.72436514, result['Var1_Std', 7])
        self.assertAlmostEqual(2.83335629, result['Var2_Std', 4])

    def test_normalize(self):
        normalize = Normalize('Var1', 'Var2')
        result = normalize.apply(self.df)
        self.assertAlmostEqual(-1.0020384, result['Var1_Std', 8])
        self.assertAlmostEqual(0.12525508, result['Var2_Std', 0])
