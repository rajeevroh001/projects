# used framework: pandas
import sys
import pandas as pd
import logging
from itertools import combinations

logger = logging.getLogger('pandas_sol')
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler(sys.stdout))


def calculate_metrics(dataframe, max_col, sum_col, filter_col, filter_condition1, filter_condition2):
    """
    function to calculate the metrics
    """
    max_v = (max_col, 'max')
    sum_value_cond1 = (sum_col, lambda x: x[dataframe[filter_col] == filter_condition1].sum())
    sum_value_cond2 = (sum_col, lambda x: x[dataframe[filter_col] == filter_condition2].sum())

    return {
        f"max_{max_col}": max_v,
        f"sum_{sum_col}_{filter_condition1}": sum_value_cond1,
        f"sum_{sum_col}_{filter_condition2}": sum_value_cond2
    }


def agg_group_by_columns(group_by_columns: [str], the_other_columns: [str]):
    """
    function to aggregate the data
    """
    agg_dict = {
        f"{column}": (column, 'count') for column in the_other_columns
    }
    agg_dict.update(calculate_metrics(df_merged, 'rating', 'value', 'status', 'ARAP', 'ACCR'))
    return df_merged.groupby(group_by_columns).agg(**agg_dict).reset_index()[desired_columns]


def union_all_dataframes(group_keys: [([str], [str])]):
    """
    Function to create dfs from all keys and returns combined result df
    """
    all_dataframes = []
    for group_by_columns, the_other_columns in group_keys:
        df_final = agg_group_by_columns(group_by_columns, the_other_columns)
        all_dataframes.append(df_final)
    return pd.concat(all_dataframes, axis=0, ignore_index=False)


def generate_pairs(*group_by_names):
    """
    function to create group keys
    """
    pairs = []
    for i in range(1, len(group_by_names)):
        for combo in combinations(group_by_names, i):
            remaining_columns = [col for col in group_by_names if col not in combo]
            if remaining_columns == ['counter_party'] or remaining_columns == ['legal_entity']: break
            pairs.append((list(combo), remaining_columns))
    return pairs


logger.info("Process started using Pandas Framework")

# Read dataset files
df1 = pd.read_csv('inputs/dataset1.csv', sep=",")
df2 = pd.read_csv('inputs/dataset2.csv', sep=",")

logger.info("Datasets read successfully")

# merge the dataframes
df_merged = pd.merge(df1, df2, on='counter_party', how='inner')

# all the desired columns
desired_columns = ['legal_entity', 'counter_party', 'tier', 'max_rating', 'sum_value_ARAP', 'sum_value_ACCR']
column_names_for_grouping = ('legal_entity', 'counter_party', 'tier')

all_pairs = generate_pairs(*column_names_for_grouping)
# eg. [(['legal_entity'], ['counter_party', 'tier']), (['counter_party'], ['legal_entity', 'tier']),...]

final_df = union_all_dataframes(all_pairs)
# ->   legal_entity counter_party  tier  max_rating  sum_value_ARAP  sum_value_ACCR
# 0           L1             6     6           6              85             100
# 1           L2             6     6           6            1020             207
# 2           L3             6     6           6             145             205

# Save results to output file
final_df.to_csv("outputs/pandas_output.csv", index=False)
logger.info("Process completed using Pandas Framework")