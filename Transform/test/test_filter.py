from unittest import TestCase
from DataModel import DataFrame
from Transform import Filter


class TestFilter(TestCase):
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

    def test_simpleEqualFilter(self):
        simple_equal_filter = Filter(Cat="=='A'")
        result = simple_equal_filter.apply(self.df)
        self.assertEqual(result['VarNone'],[87,99,None])

    def test_multipleEqualFilter(self):
        multiple_equal_filter = Filter(Cat="=='A'",Date="=='2021-03-02'")
        result = multiple_equal_filter.apply(self.df)
        self.assertEqual(result[None,0], ["A","2021-03-02",14,245,99,2,"8"])

    def test_simpleNonEqualFilter(self):
        simple_nonequal_filter = Filter(Var1=">10")
        result = simple_nonequal_filter.apply(self.df)
        self.assertEqual(result['Var1'], [14, 13, 22, 28, 23, 30])
