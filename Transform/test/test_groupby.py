import unittest
from DataModel import DataFrame
from Transform import GroupBy


class TestGroupBy(unittest.TestCase):
    def setUp(self):
        self.df = DataFrame({'Cat': ["A", "A", "A", "B", "B", "B", "B", "C", "C", "C"],
                             'Date': ["2021-03-01", "2021-03-02", "2021-03-03",
                                      "2021-03-01", "2021-03-02", "2021-03-03", "2021-03-04",
                                      "2021-03-01", "2021-03-02", "2021-03-03"],
                             'Reg': ["1", "1", "2", "1", "1", "2", "2", "1", "2", "2"],
                             'Var1': [10, 14, 13, 22, 28, 23, 30, 6, 8, 9],
                             'Var2': [250, 245, 209, 360, 328, 359, 372, 74, 78, 80],
                             'VarNone': [87, 99, None, 120, 128, None, 99, None, None, None],
                             'VarMixed': [-3, 2, "Null", 0, None, "Null", 5, None, "Null", "Null"],
                             'VarTextNum': ["5", "8", "-1", "0", "7.4", "11.9", "-8.44", "5", -4.8, 9.2]})

    def test_setOneGroup(self):
        set_one_group = GroupBy('Cat')
        result = set_one_group.apply(self.df)
        self.assertEqual(result.groups, [1, 1, 1, 2, 2, 2, 2, 3, 3, 3])

    def test_setTwoGroups(self):
        set_two_groups = GroupBy('Cat', 'Reg')
        result = set_two_groups.apply(self.df)
        self.assertEqual(result.groups, [1, 1, 2, 3, 3, 4, 4, 5, 6, 6])


if __name__ == '__main__':
    unittest.main()
