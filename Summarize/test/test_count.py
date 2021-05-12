import unittest
from DataModel import DataFrame
from Summarize import Count
from Transform import GroupBy
from Pipeline import Pipeline


class TestCount(unittest.TestCase):
    def setUp(self):
        self.df = DataFrame({"Cat": ["A", "A", "A", "B", "B", "B", "C", "C"],
                             "Var1": [348, 402, 397, 380, 589, 520, 620, 289],
                             "Var2": [74, 81, 85, 71, 102, 99, 101, 76],
                             "Var3": [74, None, 85, 71, "NaN", 99, 101, 76]})

    def test_count_var1(self):
        self.assertEqual(Count("Var1").apply(self.df)["Var1_Count"], [len(self.df["Var1"])])

    def test_count_var1_group_by(self):
        self.assertEqual(Pipeline(GroupBy("Cat"), Count("Var1")).apply(self.df)["Var1_Count"][0], 3)
        self.assertEqual(Pipeline(GroupBy("Cat"), Count("Var1")).apply(self.df)["Var1_Count"][1], 3)
        self.assertEqual(Pipeline(GroupBy("Cat"), Count("Var1")).apply(self.df)["Var1_Count"][2], 2)

    def test_count_var2(self):
        self.assertEqual(Count("Var2").apply(self.df)["Var2_Count"], [len(self.df["Var2"])])

    def test_count_var2_group_by(self):
        self.assertEqual(Pipeline(GroupBy("Cat"), Count("Var2")).apply(self.df)["Var2_Count"][0], 3)
        self.assertEqual(Pipeline(GroupBy("Cat"), Count("Var2")).apply(self.df)["Var2_Count"][1], 3)
        self.assertEqual(Pipeline(GroupBy("Cat"), Count("Var2")).apply(self.df)["Var2_Count"][2], 2)

    def test_count_var3_with_nan_na(self):
        self.assertEqual(Count("Var3", delete_nan=False, delete_na=False).apply(self.df)["Var3_Count"],
                         [len(self.df["Var3"])])

    def test_count_var3_without_nan_na(self):
        self.assertEqual(Count("Var3").apply(self.df)["Var3_Count"], [len(self.df["Var3"])-2])


if __name__ == '__main__':
    unittest.main()
