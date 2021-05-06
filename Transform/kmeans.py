from Pipeline import OnVars, Pipeline
from Transform import TransformOnGroups, Normalize, Select, Rename, GroupBy
from Summarize import Min, Max, Average
from copy import deepcopy
from random import uniform
from math import sqrt


class KMeans(OnVars, TransformOnGroups):
    def __init__(self, clusters, *on_vars, normalize=True, max_iter=1000):
        super().__init__(*on_vars)
        self.__clusters = clusters
        self.__normalize = normalize
        self.__max_iter = max_iter

    def _operation(self, group_df):
        result = deepcopy(group_df)
        base = Select(*self.vars).apply(result)
        if self.__normalize:
            normalize = Normalize(*self.vars)
            var_std = []
            var_keys = {}
            for var in self.vars:
                var_std.append(str(var) + "_Std")
                var_keys[var] = str(var) + "_Std"
            select = Select(*var_std)
            rename = Rename(**var_keys)
            normalize_pipeline = Pipeline(normalize, select, rename)
            base = normalize_pipeline.apply(base)
        mins = Min(*self.vars).apply(base)
        maxs = Max(*self.vars).apply(base)
        centers = []
        for i in range(self.__clusters):
            new_center = []
            for var in base.vars:
                new_center.append(uniform(mins[var + "_Min", 0], maxs[var + "_Max", 0]))
            centers.append(new_center)
        cluster = [None] * len(base)
        continue_loop = True
        iteration = 0
        while continue_loop:
            new_cluster = []
            iteration += 1
            for row in base:
                best_distance = None
                best_cluster = None
                for i in range(len(centers)):
                    distance = self.__euclidean_distance(row, centers[i])
                    if best_distance is None or (distance < best_distance):
                        best_distance = distance
                        best_cluster = i
                new_cluster.append(best_cluster)
            if new_cluster == cluster:
                continue_loop = False
            else:
                new_centers = deepcopy(base)
                new_centers.add_column("Partition", new_cluster)
                group = GroupBy("Partition")
                averages = Average(*base.vars)
                compute_centers = Pipeline(group, averages)
                new_centers = compute_centers.apply(new_centers)
                for row in new_centers:
                    centers[row[0]] = row[1:]
            cluster = new_cluster
            if iteration > self.__max_iter:
                continue_loop = False
        result.add_column("Partition", cluster)
        return result

    @staticmethod
    def __euclidean_distance(point_x, point_y):
        distance = 0
        for xi, yi in zip(point_x, point_y):
            distance += (yi - xi)**2
        distance = sqrt(distance)
        return distance
