from unittest import TestCase
from DataModel import DataFrame
from Summarize import Sum
from Transform import GroupBy
from Pipeline import Pipeline


class TestSum(TestCase):
    def setUp(self):
        self.df = DataFrame({"Cat": ["A", "A", "A", "B", "B", "B", "C", "C"],
                             "Var1": [348, 402, 397, 380, 589, 520, 620, 289],
                             "Var2": [74, 81, 85, 71, 102, 99, 101, 76]})

    def test_sum_var1(self):
        self.assertEqual(Sum("Var1").apply(self.df)["Var1_Sum"], [3545])

    def test_sum_var1_group_by(self):
        self.assertEqual(Pipeline(GroupBy("Cat"), Sum("Var1")).apply(self.df)["Var1_Sum"][0], 1147)
        self.assertEqual(Pipeline(GroupBy("Cat"), Sum("Var1")).apply(self.df)["Var1_Sum"][1], 1489)
        self.assertEqual(Pipeline(GroupBy("Cat"), Sum("Var1")).apply(self.df)["Var1_Sum"][2], 909)

    def test_sum_var2(self):
        self.assertEqual(Sum("Var2").apply(self.df)["Var2_Sum"], [689])

    def test_sum_var2_group_by(self):
        self.assertEqual(Pipeline(GroupBy("Cat"), Sum("Var2")).apply(self.df)["Var2_Sum"][0], 240)
        self.assertEqual(Pipeline(GroupBy("Cat"), Sum("Var2")).apply(self.df)["Var2_Sum"][1], 272)
        self.assertEqual(Pipeline(GroupBy("Cat"), Sum("Var2")).apply(self.df)["Var2_Sum"][2], 177)
