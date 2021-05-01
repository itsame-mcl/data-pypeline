from Pipeline import OnVars
from Transform import TransformOnGroups
from DataModel import DataFrame


class Sort(OnVars, TransformOnGroups):
    """
    Implements the merge sort algorithm for DataFrame transformation.

    ...

    Attributes
    ----------
    self.__vars : str
        Ordered list of the sorting vars criteria. Use the "desc_" prefix before the name of a variable
        to perform a descending sorting on this variable.

    Methods
    -------
    _operation(group_df) : DataFrame
        Execute the sorting algorithm on the group_df object, assuming this DataFrame represents a single group

    __merge_sort(nested_list, index_criteria) : list
        Entry point for the merge sort algorithm based on a nested list (first level : rows, second level : columns)
        and the list of sorting criteria columns indexes. This list is shifted of 1 (i.e. for the real index
        5, the index_critera will be 6) and negative for descending sorting (i.e. a descending sorting on the column
        0 will be coded -1).

    __merge(nested_list_a, nested_list_b, index_criteria) : list
        Performs the merge step of the merge sort algorithm between two ordered nested lists.

    __compare_lists(list_a, list_b, index_criteria) : bool
        Will be True if list_a should be inserted before list_b and False on the contrary, based on the
        variables defined by index_criteria
    """
    def _operation(self, group_df):
        nested_group_list = []
        for row in group_df:
            nested_group_list.append(row)
        index_criteria = []
        for var in self.vars:
            is_desc = False
            if var.startswith("desc_"):
                is_desc = True
                var = var[5:]
            index_criterion = group_df.vars.index(var) + 1
            if is_desc:
                index_criterion *= -1
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
            is_desc = False
            if index_criterion < 0:
                is_desc = True
                index_criterion *= -1
            index_criterion -= 1
            if is_desc:
                if list_a[index_criterion] > list_b[index_criterion]:
                    result = True
                    break
                elif list_a[index_criterion] < list_b[index_criterion]:
                    result = False
                    break
                else:
                    continue
            else:
                if list_a[index_criterion] < list_b[index_criterion]:
                    result = True
                    break
                elif list_a[index_criterion] > list_b[index_criterion]:
                    result = False
                    break
                else:
                    continue
        return result
