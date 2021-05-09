import unittest
from DataModel import DataFrame
from Summarize import Min


class TestMin(unittest.TestCase):
    def setUp(self):
        self.df = DataFrame({"Id": ["01", "02", "03", "04", "05", "06", "07", "08"],
                             "Var1": [348, 402, 397, 380, 589, 520, 620, 289],
                             "Var2": [74, 81, 85, 71, 102, 99, 101, 76]})

    def test_min_var1(self):
        self.assertEqual(Min(self.df["Var1"][0]), [289])

    def test_min_var2(self):
        self.assertEqual(Min(self.df["Var2"][0]), [71])


if __name__ == "__main__":
    unittest.main()
