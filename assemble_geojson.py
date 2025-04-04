import datetime
import json
import os

import geopandas
import pandas
from tqdm import tqdm

# REGIONS = ["BOS", "CHI", "LA", "PHL", "SFO", "WAS"]
REGIONS = ["CHI"]
DATA_FOLDER = "/home/willem/Documents/Project/TED/data/"

dfs = []
for region in REGIONS:
    for tod in ["WEDAM", "WEDPM", "SATAM"]:
        print("REG:", region)
        print("TOD:", tod)
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

                    out_gdf = geopandas.GeoDataFrame(
                        out, crs="EPSG:4326", geometry=out.geometry
                    )

                    # Write the line-delimited GeoJSON
                    jd = json.loads(out_gdf.to_crs(epsg=4326).to_json())
                    with open(
                        os.path.join(
                            DATA_FOLDER,
                            "upload",
                            "geojson",
                            f"{region}_{date.strftime('%Y%m%d')}_{tod}.geojson.nl",
                        ),
                        "w",
                    ) as outfile:
                        for feature in jd["features"]:
                            outfile.write(json.dumps(feature))
                            outfile.write("\n")
