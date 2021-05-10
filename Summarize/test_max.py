import unittest
from DataModel import DataFrame
from Summarize import Max
from Transform import GroupBy
from Pipeline import Pipeline


class TestMin(unittest.TestCase):
    def setUp(self):
        self.df = DataFrame({"Id": ["01", "02", "03", "04", "05", "06", "07", "08"],
                             "Cat": ["A", "A", "A", "B", "B", "B", "C", "C"],
                             "Var1": [348, 402, 397, 380, 589, 520, 620, 289],
                             "Var2": [74, 81, 85, 71, 102, 99, 101, 76]})

    def test_min_var1(self):
        self.assertEqual(Max("Var1").apply(self.df)["Var1_Max"], [620])

    def test_min_var1_group_by(self):
        self.assertEqual(Pipeline(GroupBy("Cat"), Max("Var1")).apply(self.df)["Var1_Max"][0], 402)
        self.assertEqual(Pipeline(GroupBy("Cat"), Max("Var1")).apply(self.df)["Var1_Max"][1], 589)
        self.assertEqual(Pipeline(GroupBy("Cat"), Max("Var1")).apply(self.df)["Var1_Max"][2], 620)

    def test_min_var2(self):
        self.assertEqual(Max("Var2").apply(self.df)["Var2_Max"], [102])

    def test_min_var2_group_by(self):
        self.assertEqual(Pipeline(GroupBy("Cat"), Max("Var2")).apply(self.df)["Var2_Max"][0], 85)
        self.assertEqual(Pipeline(GroupBy("Cat"), Max("Var2")).apply(self.df)["Var2_Max"][1], 102)
        self.assertEqual(Pipeline(GroupBy("Cat"), Max("Var2")).apply(self.df)["Var2_Max"][2], 101)


if __name__ == "__main__":
    unittest.main()
