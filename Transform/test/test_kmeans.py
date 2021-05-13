from unittest import TestCase
from DataModel import DataFrame
from Transform import KMeans


class TestKMeans(TestCase):
    def setUp(self):
        self.df = DataFrame({'X': [2, 4, 3.5, -3, -1.5, 1, 0.75, -2, -0.5, -3.5,
                                   -5, 2.5, 5, -1, 3.5, -1.75, 3, 4, -3.5, -4],
                             'Y': [1, 3, -2.5, 2.5, -3, 5, -0.5, 4, 1, -2.5,
                                   -4, -4.5, 0.75, 1.5, -5, -2.5, 2.5, -2, 2, -1.25]})

    def test_kmeans(self):
        kmeans_4cl = KMeans(4, 'X', 'Y', random_seed=4)
        result = kmeans_4cl.apply(self.df)
        self.assertEqual([3, 3, 1, 2, 0, 3, 1, 2, 2, 0, 0, 1, 3, 2, 1, 0, 3, 1, 2, 0], result['Partition'])
