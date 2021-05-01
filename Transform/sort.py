from Pipeline import OnVars
from Transform import TransformOnGroups
from DataModel import DataFrame


class Sort(OnVars, TransformOnGroups):
    def _operation(self, group_df):
        nested_group_list = []
        for row in group_df:
            nested_group_list.append(row)
        index_criteria = []
        for var in self.vars:
            index_criterion = group_df.vars.index(var)
            index_criteria.append(index_criterion)
        sorted_group_list = Sort.__merge_sort(nested_group_list, index_criteria)
        result = DataFrame()
        for var in group_df.vars:
            result.add_column(var)
        for row in sorted_group_list:
            result.add_row(row)
        return result

    @staticmethod
    def __merge_sort(nested_list, index_criteria):
        if len(nested_list) <= 1:
            return nested_list
        else:
            split_point = len(nested_list)//2
            return Sort.__merge(Sort.__merge_sort(nested_list[0:split_point], index_criteria),
                                Sort.__merge_sort(nested_list[split_point:len(nested_list)], index_criteria),
                                index_criteria)

    @staticmethod
    def __merge(nested_list_a, nested_list_b, index_criteria):
        if len(nested_list_a) == 0:
            return nested_list_b
        elif len(nested_list_b) == 0:
            return nested_list_a
        elif Sort.__compare_lists(nested_list_a[0], nested_list_b[0], index_criteria):
            result = [nested_list_a[0]]
            result.extend(Sort.__merge(nested_list_a[1:], nested_list_b, index_criteria))
            return result
        else:
            result = [nested_list_b[0]]
            result.extend(Sort.__merge(nested_list_a, nested_list_b[1:], index_criteria))
            return result

    @staticmethod
    def __compare_lists(list_a, list_b, index_criteria):
        result = True
        for index_criterion in index_criteria:
            if list_a[index_criterion] < list_b[index_criterion]:
                result = True
                break
            elif list_a[index_criterion] > list_b[index_criterion]:
                result = False
                break
            else:
                continue
        return result
