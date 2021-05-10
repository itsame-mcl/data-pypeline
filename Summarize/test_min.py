import unittest
from DataModel import DataFrame
from Summarize import Min
from Transform import GroupBy
from Pipeline import Pipeline


class TestMin(unittest.TestCase):
    def setUp(self):
        self.df = DataFrame({"Id": ["01", "02", "03", "04", "05", "06", "07", "08"],
                             "Cat": ["A", "A", "A", "B", "B", "B", "C", "C"],
                             "Var1": [348, 402, 397, 380, 589, 520, 620, 289],
                             "Var2": [74, 81, 85, 71, 102, 99, 101, 76]})

    def test_min_var1(self):
        self.assertEqual(Min("Var1").apply(self.df)["Var1_Min"], [289])

    def test_min_var1_group_by(self):
        self.assertEqual(Pipeline(GroupBy("Cat"), Min("Var1")).apply(self.df)["Var1_Min"][0], 348)
        self.assertEqual(Pipeline(GroupBy("Cat"), Min("Var1")).apply(self.df)["Var1_Min"][1], 380)
        self.assertEqual(Pipeline(GroupBy("Cat"), Min("Var1")).apply(self.df)["Var1_Min"][2], 289)

    def test_min_var2(self):
        self.assertEqual(Min("Var2").apply(self.df)["Var2_Min"], [71])

    def test_min_var2_group_by(self):
        self.assertEqual(Pipeline(GroupBy("Cat"), Min("Var2")).apply(self.df)["Var2_Min"][0], 74)
        self.assertEqual(Pipeline(GroupBy("Cat"), Min("Var2")).apply(self.df)["Var2_Min"][1], 71)
        self.assertEqual(Pipeline(GroupBy("Cat"), Min("Var2")).apply(self.df)["Var2_Min"][2], 76)


if __name__ == "__main__":
    unittest.main()
