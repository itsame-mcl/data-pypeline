import unittest
from DataModel import DataFrame
from Summarize import Variance
from Transform import GroupBy
from Pipeline import Pipeline
from numpy import var
from math import sqrt


class TestVariance(unittest.TestCase):
    def setUp(self):
        self.df = DataFrame({"Id": ["01", "02", "03", "04", "05", "06", "07", "08"],
                             "Cat": ["A", "A", "A", "A", "B", "B", "B", "B"],
                             "Var1": [10, 10, 10, 10, 10, 10, 10, 10],
                             "Var2": [10, 10, 11, 11, 13, 13, 13, 13]})

    def test_variance_var1_without_sd(self):
        self.assertEqual(Variance("Var1", get_sd=False).apply(self.df)["Var1_Var"], [var(self.df["Var1"])])

    def test_sd_var1_without_variance(self):
        self.assertEqual(Variance("Var1", get_var=False).apply(self.df)["Var1_SD"], [sqrt(var(self.df["Var1"]))])

    def test_variance_var1_without_sd_group_by(self):
        self.assertEqual(Pipeline(GroupBy("Cat"), Variance("Var1", get_sd=False)).apply(self.df)["Var1_Var"][0], 0)
        self.assertEqual(Pipeline(GroupBy("Cat"), Variance("Var1", get_sd=False)).apply(self.df)["Var1_Var"][1], 0)

    def test_sd_var1_without_variance_group_by(self):
        self.assertEqual(Pipeline(GroupBy("Cat"), Variance("Var1", get_var=False)).apply(self.df)["Var1_SD"][0], 0)
        self.assertEqual(Pipeline(GroupBy("Cat"), Variance("Var1", get_var=False)).apply(self.df)["Var1_SD"][1], 0)

    def test_variance_var2_without_sd(self):
        self.assertEqual(Variance("Var2", get_sd=False).apply(self.df)["Var2_Var"], [var(self.df["Var2"])])

    def test_sd_var2_without_variance(self):
        self.assertEqual(Variance("Var2", get_var=False).apply(self.df)["Var2_SD"], [sqrt(var(self.df["Var2"]))])

    def test_variance_var2_without_sd_group_by(self):
        self.assertEqual(Pipeline(GroupBy("Cat"), Variance("Var2", get_sd=False)).apply(self.df)["Var2_Var"][0], 0.25)
        self.assertEqual(Pipeline(GroupBy("Cat"), Variance("Var2", get_sd=False)).apply(self.df)["Var2_Var"][1], 0)

    def test_sd_var2_without_variance_group_by(self):
        self.assertEqual(Pipeline(GroupBy("Cat"), Variance("Var2", get_var=False)).apply(self.df)["Var2_SD"][0], 0.50)
        self.assertEqual(Pipeline(GroupBy("Cat"), Variance("Var2", get_var=False)).apply(self.df)["Var2_SD"][1], 0)


if __name__ == '__main__':
    unittest.main()
