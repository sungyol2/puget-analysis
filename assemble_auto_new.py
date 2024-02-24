import os

import pandas
import geopandas
import zipfile

AUTO_DATA_FOLDER = "/home/willem/Documents/Project/TED/data/raw/auto"
REGION = "BOS"
TOD = "SATAM"
AUTO_OUT_FILE = (
    f"/home/willem/Documents/Project/TED/data/region/{REGION}/auto/{TOD}.parquet"
)
GDB_FILENAME = "/home/willem/Documents/Project/TED/data/ted_streetmaps.gdb.zip"
GDB_LAYER = "BOS3_centroids_SolveLargeODCostMatrix"

# Mapping file

gdb_bg = geopandas.read_file(GDB_FILENAME, layer=GDB_LAYER)[
    ["BG20", "DestinationOID", "SourceOID"]
]

z = zipfile.ZipFile(os.path.join(AUTO_DATA_FOLDER, f"outputs_{REGION}_{TOD}.zip"))

dfs = []
for f in z.namelist():
    if f.endswith(".arrow"):
        df = pandas.read_feather(z.open(f))
        df = pandas.merge(gdb_bg, df, left_on="SourceOID", right_on="OriginOID")
        print(df.head())
        dfs.append(df)

        input()

all = pandas.concat(dfs, axis="index")
all = all.rename(
    columns={"Total_Time": "travel_time", "i_id": "from_id", "j_id": "to_id"}
)
all[["from_id", "to_id", "travel_time"]].to_parquet(AUTO_OUT_FILE)
print(all.head())
