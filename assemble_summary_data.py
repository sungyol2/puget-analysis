import datetime
import json
import os

import geopandas
import pandas
from tqdm import tqdm

REGIONS = ["BOS", "PHL", "CHI", "LA", "NYC", "SFO", "WAS"]
DATA_FOLDER = "/home/willem/Documents/Project/TED/data/"

# PACKAGE SUMMARIES

for region in REGIONS:
    for tod in ["WEDAM", "WEDPM", "SATAM"]:
        dfs = []
        for run in os.listdir(os.path.join(DATA_FOLDER, "results")):
            if run.endswith(region):
                print(run)
                summary_file = os.path.join(
                    DATA_FOLDER, "results", run, region, tod, "summary.csv"
                )
                if os.path.exists(summary_file):
                    date = datetime.datetime.strptime(run, f"%Y-%m-%d-{region}")
                    # print("Running", summary_file)
                    df = pandas.read_csv(summary_file)
                    df["date"] = date
                    dfs.append(df)
                else:
                    print("Missing summary file for", run)

        all = pandas.concat(dfs, axis="index")

        all.to_csv(
            os.path.join(
                DATA_FOLDER,
                "packaged",
                region,
                "summary",
                f"summary_{region}_{tod}.csv",
            ),
            index=False,
        )
