import datetime
import json
import os

import geopandas
import pandas
from tqdm import tqdm

REGION = "CHI"
TOD = "WEDAM"
DATA_FOLDER = "/home/willem/Documents/Project/TED/data/"

# PACKAGE SUMMARIES
dfs = []
for run in os.listdir(os.path.join(DATA_FOLDER, "results")):
    if run.endswith(REGION):
        summary_file = os.path.join(
            DATA_FOLDER, "results", run, REGION, TOD, "summary.csv"
        )
        if os.path.exists(summary_file):
            date = datetime.datetime.strptime(run, f"%Y-%m-%d-{REGION}")
            df = pandas.read_csv(summary_file)
            df["date"] = date
            dfs.append(df)

all = pandas.concat(dfs, axis="index")
all.to_csv(
    os.path.join(
        DATA_FOLDER, "packaged", REGION, "summary", f"summary_{REGION}_{TOD}.csv"
    ),
    index=False,
)


# PACKAGE ALL ACCESS FILES

db = geopandas.read_file(
    os.path.join(DATA_FOLDER, "region", REGION, f"{REGION}.gpkg"), layer="bg_areas"
)

runs = [
    r for r in os.listdir(os.path.join(DATA_FOLDER, "results")) if r.endswith(REGION)
]

for run in tqdm(runs, total=len(runs)):
    if run.endswith(REGION):
        date = datetime.datetime.strptime(run, f"%Y-%m-%d-{REGION}")
        tsi = pandas.read_csv(
            os.path.join(DATA_FOLDER, "results", run, REGION, "tsi.csv"),
            dtype={"BG20": str},
        )
        for tod in ["WEDAM", "WEDPM", "SATAM"]:
            acs = pandas.read_csv(
                os.path.join(DATA_FOLDER, "results", run, REGION, tod, "access.csv"),
                dtype={"BG20": str},
            )
            out = pandas.merge(acs, tsi[["BG20", tod]], on="BG20").rename(
                columns={tod: "tsi"}
            )
            out = pandas.merge(out, db, on="BG20")
            # Write the CSV files
            csv_out = out.drop(columns="geometry")
            csv_out.to_csv(
                os.path.join(
                    DATA_FOLDER,
                    "packaged",
                    REGION,
                    "scores",
                    f"acs_{REGION}_{date.strftime('%Y%m%d')}_{tod}.csv",
                ),
                index=False,
            )

            out_gdf = geopandas.GeoDataFrame(
                out, crs="EPSG:4326", geometry=out.geometry
            )

            # Write the geopackage
            out_gdf.to_file(
                os.path.join(
                    DATA_FOLDER, "packaged", REGION, "gpkg", f"acs_{REGION}.gpkg"
                ),
                layer=f"{date.strftime('%Y%m%d')}_{tod}",
            )

            # Write the line-delimited GeoJSON
            jd = json.loads(out_gdf.to_crs(epsg=4326).to_json())
            with open(
                os.path.join(
                    DATA_FOLDER,
                    "upload",
                    "geojson",
                    f"{REGION}_{date.strftime('%Y%m%d')}_{tod}.geojson.nl",
                ),
                "w",
            ) as outfile:
                for feature in jd["features"]:
                    outfile.write(json.dumps(feature))
                    outfile.write("\n")
