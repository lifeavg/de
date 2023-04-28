import pandas as pd

data = pd.read_csv("/home/lifeavg/Downloads/database.csv", parse_dates=["departure"])

data = data.sort_values(["departure"]).groupby([data["departure"].dt.year, data["departure"].dt.month])

for group_index, group_dataframe in data:
    group_dataframe.to_csv(
        f"/home/lifeavg/Downloads/data/helsinki_bikes_{group_index[0]}_{group_index[1]}.csv",
        index=False,
    )
