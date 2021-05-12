import unittest
from DataModel import DataFrame
from Transform import Sort


class TestMutate(unittest.TestCase):
    def setUp(self):
        self.df = DataFrame({'Cat': ["A", "A", "A", "B", "B", "B", "B", "C", "C", "C"],
                             'Date': ["2021-03-01", "2021-03-02", "2021-03-03",
                                      "2021-03-01", "2021-03-02", "2021-03-03", "2021-03-04",
                                      "2021-03-01", "2021-03-02", "2021-03-03"],
                             'Var1': [10, 14, 13, 22, 28, 23, 30, 6, 8, 9],
                             'Var2': [250, 245, 209, 360, 328, 359, 372, 74, 78, 80],
                             'VarNone': [87, 99, None, 120, 128, None, 99, None, None, None],
                             'VarMixed': [-3, 2, "Null", 0, None, "Null", 5, None, "Null", "Null"],
                             'VarTextNum': ["5", "8", "-1", "0", "7.4", "11.9", "-8.44", "5", -4.8, 9.2]})

    def test_simple_sort(self):
        simple_sort = Sort('Var1')
        result = simple_sort.apply(self.df)
        self.assertEqual([74, 78, 80, 250, 209, 245, 360, 359, 328, 372], result['Var2'])

    def test_simple_desc_sort(self):
        simple_desc_sort = Sort('desc_Var2')
        result = simple_desc_sort.apply(self.df)
        self.assertEqual([30, 22, 23, 28, 10, 14, 13, 9, 8, 6], result['Var1'])

    def test_multiple_sort(self):
        multiple_sort = Sort('Date', 'Var1')
        result = multiple_sort.apply(self.df)
        self.assertEqual([74, 250, 360, 78, 245, 328, 80, 209, 359, 372], result['Var2'])


if __name__ == '__main__':
    unittest.main()
