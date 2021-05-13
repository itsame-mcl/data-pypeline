from unittest import TestCase
from DataModel import DataFrame
from Transform import MovingAverage


class TestMovingAverage(TestCase):
    def setUp(self):
        self.df = DataFrame({'Dep': ["1", "1", "1", "1", "1", "2", "2", "2", "2", "2"],
                             'Jour': ["2021-01-01", "2021-01-02", "2021-01-03", "2021-01-04",
                                      "2021-01-05", "2020-11-16", "2020-11-17", "2020-11-18",
                                      "2020-11-19", "2020-11-20"],
                             'Var': [248, 245, 209, 359, 326, 86, 92, 74, 80, 77],
                             'VarNone': [87, 99, None, 120, 128, None, 99, None, None, None],
                             'VarMixed': [-3, 2, "Null", 0, None, "Null", 5, None, "Null", "Null"],
                             'VarTextNum': ["5", "8", "-1", "0", "7.4", "11.9", "-8.44", "5", -4.8, 9.2]})

    def test_ungroup_ma(self):
        ungroup_ma = MovingAverage(3, 'Jour', 'Var')
        result = ungroup_ma.apply(self.df)
        self.assertEqual([None, None, 84, 82, 77, 135, 190, 234, 271, 298], result['Var_MA3'])

    def test_group_ma(self):
        self.df.add_group('Dep')
        group_ma = MovingAverage(3, 'Jour', 'Var')
        result = group_ma.apply(self.df)
        self.assertEqual([None, None, 234, 271, 298, None, None, 84, 82, 77], result['Var_MA3'])
