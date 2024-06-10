import os
import json

import pandas as pd
import geopandas as gpd


DATA_FOLDER = "../data"
REGIONS = ["BOS", "CHI", "LA", "NYC", "PHL", "SFO", "WAS"]
REGIONS = ["BOS", "CHI", "LA", "NYC", "PHL", "SFO"]
COLUMNS = [
    "B03002_001E",
    "B03002_003E",
    "B03002_004E",
    "B03002_006E",
    "B03002_012E",
    "B11003_016E",
    "age_65p",
    "low_income",
    "zero_car_hhld",
]

RATIOS = {
    "B03002_001E": 100,
    "B03002_003E": 100,
    "B03002_004E": 100,
    "B03002_006E": 100,
    "B03002_012E": 100,
    "B11003_016E": 50,
    "age_65p": 100,
    "low_income": 100,
    "zero_car_hhld": 100,
}


for region in REGIONS:
    print("Running", region)
    gdf = gpd.read_file(
        os.path.join(DATA_FOLDER, "region", region, f"{region}.gpkg"),
        driver="GPKG",
        layer="bg_areas",
    )
    demo = pd.read_csv(
        os.path.join(DATA_FOLDER, "region", region, "demographics.csv"),
        dtype={"BG20": str},
    )
    gdf = gdf.merge(demo, on="BG20")
    for column in COLUMNS:
        print("  Column", column)
        out = gdf.copy()
        out["geometry"] = out.geometry.sample_points(
            (out[column] / RATIOS[column]).round().astype(int)
        )
        
        # Remove all data columns as they are not needed.
        out = out[["BG20", "geometry"]]
        
        # Write the standard GeoJSON (for website DL)
        out.to_crs(epsg=4326).to_file(
            os.path.join(
                DATA_FOLDER, "upload", "dots", "website", f"{region}_{column}.geojson"
            )
        )
        
        # Write the line-delimited GeoJSON
        jd = json.loads(out.to_crs(epsg=4326).to_json())
        with open(
            os.path.join(
                DATA_FOLDER,
                "upload",
                "dots",
                "mapbox",
                f"{region}_{column}.geojson.nl",
            ),
            "w",
        ) as outfile:
            for feature in jd["features"]:
                outfile.write(json.dumps(feature))
                outfile.write("\n")
