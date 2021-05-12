import unittest
from DataModel import DataFrame
from Transform import Join


class TestGroupBy(unittest.TestCase):
    def setUp(self):
        self.df1 = DataFrame({'Reg': ["1", "2", "3", "4", "5", "6", "7", "8", "9", "9"],
                             'Var1': [10, 14, 13, 22, 28, 23, 30, 6, 8, 9],
                             'Var2': [250, 245, 209, 360, 328, 359, 372, 74, 78, 80],
                             'VarNone': [87, 99, None, 120, 128, None, 99, None, None, None],
                             'VarMixed': [-3, 2, "Null", 0, None, "Null", 5, None, "Null", "Null"],
                             'VarTextNum': ["5", "8", "-1", "0", "7.4", "11.9", "-8.44", "5", -4.8, 9.2]})
        self.df2 = DataFrame({'Region': ["1", "2", "3", "4", "5", "5", "7", "8", "9", "10"],
                             'Nom': ["A","B","C","D","E","Ebis","G","H","I","J"],
                             'Var1':[22, 44, 84, 16, 7, 99, 11, 14, 29, 22]})

    def test_joinTables(self):
        join_tables = Join(self.df2, Region='Reg')
        result = join_tables.apply(self.df1)
        self.assertEqual(["A", "B", "C", "D", "E", "Ebis", None, "G", "H", "I", "I"], result["Nom"])
        self.assertEqual(['Reg','Var1','Var2','VarNone','VarMixed','VarTextNum','Nom','Y_Var1'], result.vars)


if __name__ == '__main__':
    unittest.main()
