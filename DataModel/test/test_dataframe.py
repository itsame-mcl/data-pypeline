import unittest
from DataModel import DataFrame


# noinspection PyTypeChecker
class TestDataFrame(unittest.TestCase):
    def setUp(self):
        self.df = DataFrame({'Cat': ["A", "A", "A", "B", "B", "B", "B", "C", "C", "C"],
                             'Date': ["2021-03-01", "2021-03-02", "2021-03-03",
                                      "2021-03-01", "2021-03-02", "2021-03-03", "2021-03-04",
                                      "2021-03-01", "2021-03-02", "2021-03-03"],
                             'Var1': [10, 14, 13, 22, 28, 23, 30, 6, 8, 9],
                             'Var2': [250, 245, 209, 360, 328, 359, 372, 74, 78, 80],
                             'VarNone': [87, 99, None, 120, 128, None, 99, None, None, None],
                             'VarMixed': [-3, 2, "Null", 0, None, "Null", 5, None, "Null", "Null"]})

    def test_init_emptyDataFrame(self):
        df = DataFrame()
        self.assertIsInstance(df, DataFrame)
        self.assertEqual(len(df), 0)
        self.assertEqual(len(df.vars), 0)

    def test_getItem_singleColumn_DataFrame(self):
        self.assertEqual(self.df['Cat'], ["A", "A", "A", "B", "B", "B", "B", "C", "C", "C"])
        self.assertEqual(self.df[2], [10, 14, 13, 22, 28, 23, 30, 6, 8, 9])
        self.assertEqual(self.df[4, ], [87, 99, None, 120, 128, None, 99, None, None, None])
        self.assertRaises(KeyError, lambda: self.df['Outside'])
        self.assertRaises(IndexError, lambda: self.df[8])
        self.assertRaises(TypeError, lambda: self.df[5.5])

    def test_shapeDataFrame(self):
        self.assertEqual(self.df.shape, (6, 10))

    def test_lenDataFrame(self):
        self.assertEqual(len(self.df), 10)


if __name__ == '__main__':
    unittest.main()
