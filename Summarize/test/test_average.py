import unittest
from statistics import mean
from DataModel import DataFrame
from Summarize import Average
from Transform import GroupBy
from Pipeline import Pipeline


class TestAverage(unittest.TestCase):
    def setUp(self):
        self.df = DataFrame({"Cat": ["A", "A", "A", "B", "B", "B", "C", "C"],
                             "Var1": [98, 100, 102, 100, 200, 150, 620, 40],
                             "Var2": [74, 81, 85, 71, 103, 99, 101, 76]})

    def test_average_var1(self):
        self.assertEqual(Average("Var1").apply(self.df)["Var1_Average"], [mean(self.df["Var1"])])

    def test_average_var1_group_by(self):
        self.assertEqual(Pipeline(GroupBy("Cat"), Average("Var1")).apply(self.df)["Var1_Average"][0], 100.0)
        self.assertEqual(Pipeline(GroupBy("Cat"), Average("Var1")).apply(self.df)["Var1_Average"][1], 150.0)
        self.assertEqual(Pipeline(GroupBy("Cat"), Average("Var1")).apply(self.df)["Var1_Average"][2], 330.0)

    def test_average_var2(self):
        self.assertEqual(Average("Var2").apply(self.df)["Var2_Average"], [mean(self.df["Var2"])])

    def test_average_var2_group_by(self):
        self.assertEqual(Pipeline(GroupBy("Cat"), Average("Var2")).apply(self.df)["Var2_Average"][0], 80.0)
        self.assertEqual(Pipeline(GroupBy("Cat"), Average("Var2")).apply(self.df)["Var2_Average"][1], 91.0)
        self.assertEqual(Pipeline(GroupBy("Cat"), Average("Var2")).apply(self.df)["Var2_Average"][2], 88.5)


if __name__ == '__main__':
    unittest.main()
