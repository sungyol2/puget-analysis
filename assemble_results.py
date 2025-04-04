import datetime
import json
import os

import geopandas
import pandas
from tqdm import tqdm

# REGIONS = ["BOS", "CHI", "LA", "NYC", "PHL", "SFO", "WAS"]
REGIONS = ["CHI"]
DATA_FOLDER = "/home/willem/Documents/Project/TED/data/"
INCLUDE_SUMMARY = False
INCLUDE_SCORES = True
INCLUDE_GEOPACKAGE = True

# PACKAGE SUMMARIES
dfs = []
for region in REGIONS:
    for tod in ["WEDAM", "WEDPM", "SATAM"]:
        print("REGION:", region, "TOD:", tod)
        if INCLUDE_SUMMARY == True:
            for run in os.listdir(os.path.join(DATA_FOLDER, "results")):
                if run.endswith(region):
                    summary_file = os.path.join(
                        DATA_FOLDER, "results", run, region, tod, "summary.csv"
                    )
                    if os.path.exists(summary_file):
                        date = datetime.datetime.strptime(run, f"%Y-%m-%d-{region}")
                        df = pandas.read_csv(summary_file)
                        df["date"] = date
                        dfs.append(df)
                    else:
                        print("Missing summary file for", run)

            all = pandas.concat(dfs, axis="index")
            summary_outfile = os.path.join(
                DATA_FOLDER,
                "packaged",
                region,
                "summary",
                f"summary_{region}_{tod}.csv",
            )
            all.to_csv(
                summary_outfile,
                index=False,
            )
            print("  Summary file written to", summary_outfile)

        # PACKAGE ALL ACCESS FILES
        if INCLUDE_SCORES == True:
            db = geopandas.read_file(
                os.path.join(DATA_FOLDER, "region", region, f"{region}.gpkg"),
                layer="bg_areas",
            )

            runs = [
                r
                for r in os.listdir(os.path.join(DATA_FOLDER, "results"))
                if r.endswith(region)
            ]

            for run in tqdm(runs, total=len(runs)):
                if run.endswith(region):
                    date = datetime.datetime.strptime(run, f"%Y-%m-%d-{region}")
                    tsi = pandas.read_csv(
                        os.path.join(DATA_FOLDER, "results", run, region, "tsi.csv"),
                        dtype={"BG20": str},
                    )
                    for tod in ["WEDAM", "WEDPM", "SATAM"]:
                        acs = pandas.read_csv(
                            os.path.join(
                                DATA_FOLDER, "results", run, region, tod, "access.csv"
                            ),
                            dtype={"BG20": str},
                        )
                        out = pandas.merge(acs, tsi[["BG20", tod]], on="BG20").rename(
                            columns={tod: "tsi"}
                        )
                        out = pandas.merge(out, db, on="BG20")
                        # Let's do some rounding
                        for cname in out.columns:
                            if "t1_auto" in cname or "t3_auto" in cname:
                                # Round travel times to nearest decimal
                                out[cname] = out[cname].round().fillna(-1).astype(int)
                            if "acres_" in cname:
                                out[cname] = out[cname].round().astype(int)
                            if "C000_" in cname:
                                out[cname] = out[cname].round().astype(int)
                            if cname.endswith("_t1") or cname.endswith("_t3"):
                                out[cname] = out[cname].round().fillna(-1).astype(int)
                        # Write the CSV files
                        csv_out = out.drop(columns="geometry")
                        csv_out.to_csv(
                            os.path.join(
                                DATA_FOLDER,
                                "packaged",
                                region,
                                "scores",
                                f"acs_{region}_{date.strftime('%Y%m%d')}_{tod}.csv",
                            ),
                            index=False,
                        )
                        if INCLUDE_GEOPACKAGE == True:
                            out_gdf = geopandas.GeoDataFrame(
                                out, crs="EPSG:4326", geometry=out.geometry
                            )
                            # Write the geopackage
                            out_gdf.to_file(
                                os.path.join(
                                    DATA_FOLDER,
                                    "packaged",
                                    region,
                                    "gpkg",
                                    f"acs_{region}.gpkg",
                                ),
                                layer=f"{date.strftime('%Y%m%d')}_{tod}",
                            )
