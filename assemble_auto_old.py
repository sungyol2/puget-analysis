import os

import pandas
import zipfile

from tqdm import tqdm

AUTO_DATA_FOLDER = "/home/willem/Documents/Project/TED/data/raw/auto"
REGION = "SFO"
TOD = "SATAM"
AUTO_OUT_FILE = (
    f"/home/willem/Documents/Project/TED/data/region/{REGION}/auto/{TOD}.parquet"
)

z = zipfile.ZipFile(os.path.join(AUTO_DATA_FOLDER, f"{REGION}_{TOD}_output.zip"))

dfs = []
print("Assembling Parquet Files")
for f in tqdm(z.namelist(), total=len(z.namelist())):
    if f.endswith(".parquet"):
        df = pandas.read_parquet(z.open(f))
        df["Total_Time"] = df.Total_Time.round(0)
        dfs.append(df)

print("Concatenating")
all = pandas.concat(dfs, axis="index")
all = all.rename(
    columns={"Total_Time": "travel_time", "i_id": "from_id", "j_id": "to_id"}
)
all[["from_id", "to_id", "travel_time"]].to_parquet(AUTO_OUT_FILE)
print(all.head())
