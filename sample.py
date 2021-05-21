from IO import Import
from Summarize import Sum
from Transform import AsNumeric, Rename, GroupBy, Sort, Mutate, Filter
from Pipeline import Pipeline


if __name__ == '__main__':
    movies = Import.import_csv("data/movies.csv", delimiter=",")
    convert = AsNumeric('year', 'intgross_2013', "budget_2013")
    remove_na = Filter(intgross_2013="!='N/A'", budget_2013="!='N/A'")
    group_bechdel = GroupBy("binary")
    sums = Sum('intgross_2013', 'budget_2013')
    rename_vars = Rename(intgross_2013="intgross_2013_Sum", budget_2013="budget_2013_Sum")
    compute_ratio = Mutate(profitability_2013="intgross_2013/budget_2013")
    sort_profitability = Sort("desc_profitability_2013")
    # Profitability of Movies passing Bechdel test vs Movies failing Bechdel test
    bechdel_analysis = Pipeline(remove_na, convert, group_bechdel,
                                sums, rename_vars, compute_ratio, sort_profitability)
    result_bechdel = bechdel_analysis.apply(movies)
    print(result_bechdel)
    # Most Proficient Movie
    profitability_analysis = Pipeline(remove_na, convert, compute_ratio, sort_profitability)
    result_profitability = profitability_analysis.apply(movies)
    print(result_profitability)
