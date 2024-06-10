"""
Script to manage and generate fare itineraries and to compute fares

This script is a colleciton of data calls that are used to compute fare
itineraries and to compute fare matrices.

"""

import os

import geopandas
from gtfslite import GTFS

from ted.fare import *

DATA_FOLDER = "/home/willem/Documents/Project/TED/data/"
REGION = "BOS"
TYPE = "limited"
YEAR = "2023"
# GTFS_DATE = "2020-02-24"
# GTFS_DATE = "2023-09-25"
# START_DATETIME = datetime.datetime(2020, 2, 26, 8)
# START_DATETIME = datetime.datetime(2023, 9, 27, 8)

############################
#       FARE ANALYSIS      #
############################

# UTIL: Rename zipfiles with gtfs tag
# add_gtfs_tag_to_zipfiles("../otp/SFO-2023-09-25-limited")

# UTIL: Port data over from another folder and name it for OTP's sake
# otp_folder = f"/home/willem/Documents/Project/TED/otp/{REGION}-{GTFS_DATE}-{TYPE}"
# if TYPE == "limited":
#     gtfs_folder = os.path.join(
#         DATA_FOLDER, "region", REGION, "gtfs", "limited", f"{GTFS_DATE}-limited"
#     )
# else:
#     gtfs_folder = os.path.join(
#         DATA_FOLDER, "region", REGION, "gtfs", "full", f"{GTFS_DATE}"
#     )
# for f in os.listdir(gtfs_folder):
#     print(f)
#     z = GTFS.load_zip(os.path.join(gtfs_folder, f), ignore_optional_files="keep_shapes")
#     for c in z.routes.columns:
#         if z.routes.dtypes[c] == "object":
#             z.routes[c] = z.routes[c].str.replace("nan", "")
#     for c in z.stops.columns:
#         if z.stops.dtypes[c] == "object":
#             z.stops[c] = z.stops[c].str.replace("nan", "")
#     if "note_id" in z.stop_times.columns:
#         z.stop_times = z.stop_times.drop(columns=["note_id"])
#     for c in z.stop_times.columns:
#         if z.stop_times.dtypes[c] == "object":
#             z.stop_times[c] = z.stop_times[c].str.replace("nan", "")
#     for c in z.trips.columns:
#         if z.trips.dtypes[c] == "object":
#             z.trips[c] = z.trips[c].str.replace("nan", "")
#     if "stop_timezone" in z.stops.columns:
#         z.stops = z.stops.drop(columns=["stop_timezone"])
#     z.stop_times["arrival_time"] = z.stop_times["arrival_time"].str.replace("nan", "")
#     z.stop_times["departure_time"] = z.stop_times["departure_time"].str.replace(
#         "nan", ""
#     )
#     feed_no = f.split(".")[0].split("-")[-1]
#     z.feed_info = pandas.DataFrame(
#         {
#             "feed_publisher_name": ["Modified by WK"],
#             "feed_publisher_url": ["http://www.klumpentown.com"],
#             "feed_lang": ["en"],
#             "feed_id": [feed_no],
#         }
#     )
#     z.write_zip(os.path.join(otp_folder, "gtfs-" + f))

########### Load the cluster file
# clusters = geopandas.read_file(
#     os.path.join(DATA_FOLDER, "region", REGION, f"{REGION}.gpkg"), layer="clusters"
# )

############## Run R5 on the eixsting clusters to get a proper list
# if TYPE == "limited":
#     gtfs_date_folder = f"{GTFS_DATE}-limited"
# else:
#     gtfs_date_folder = GTFS_DATE
# run_r5_on_clusters(
#     clusters=clusters,
#     gtfs_folder=os.path.join(
#         DATA_FOLDER, "region", REGION, "gtfs", TYPE, gtfs_date_folder
#     ),
#     osm_file=os.path.join(DATA_FOLDER, "region", REGION, f"{REGION}.osm.pbf"),
#     departure=START_DATETIME,
#     output_file=os.path.join(
#         DATA_FOLDER, "region", REGION, "fare", YEAR, f"cluster_pairs_{YEAR}_{TYPE}.csv"
#     ),
# )


########## Run OTP on cluster pair list

# pairs = pandas.read_csv(
#     os.path.join(
#         DATA_FOLDER, "region", REGION, "fare", YEAR, f"cluster_pairs_{YEAR}_{TYPE}.csv"
#     )
# )

# chunk_folder = f"../chunks/{REGION}"
# for f in os.listdir(chunk_folder):
#     os.remove(os.path.join(chunk_folder, f))

# run_otp_itineraries_from_pairs_list(
#     fares_yaml=f"/home/willem/Documents/Project/TED/otp/{REGION}-{GTFS_DATE}-{TYPE}/config.yaml",
#     pairs_df=pairs,
#     clusters=clusters[["CLUSTER_ID", "MEAN_X", "MEAN_Y"]],
#     departure=START_DATETIME,
#     output_folder=chunk_folder,
#     region_key=REGION,
#     chunk_size=50,
# )

# dechunkify(
#     f"../chunks/{REGION}",
#     f"../data/region/{REGION}/fare/{YEAR}/itineraries_{YEAR}_{TYPE}.parquet",
# )

######### Get unique routes or municipalities used
# get_unique_routes_used(
#     os.path.join(
#         DATA_FOLDER,
#         "region",
#         REGION,
#         "fare",
#         YEAR,
#         f"itineraries_{YEAR}_{TYPE}.parquet",
#     ),
#     os.path.join(
#         DATA_FOLDER, "region", REGION, "fare", f"{YEAR}_unique_routes_{TYPE}.csv"
#     ),
# )

############ Compute a fare matrix from the fares
df = pandas.read_parquet(
    os.path.join(
        DATA_FOLDER,
        "region",
        REGION,
        "fare",
        YEAR,
        f"itineraries_{YEAR}_{TYPE}.parquet",
    )
).rename(columns={"mode": "transport_mode"})

pairs = df.drop_duplicates(subset=["from_id", "to_id"])

fares = {"from_id": [], "to_id": [], "fare_cost": []}

print("Making Fare Matrix")
fare_db = f"/home/willem/Documents/Project/TED/data/region/{REGION}/fare/{YEAR}/{REGION}{YEAR[-2:]}.db"
print("DB:", fare_db)
df["feed"] = df["feed"].str.replace("gtfs-", "")
df["feed"] = df["feed"].str.replace(
    "california-golden-gate-ferry-0", "california-blue-gold-fleet-1178"
)

for idx, pair in tqdm(pairs.iterrows(), total=pairs.shape[0]):
    sub_df = df[(df.from_id == pair["from_id"]) & (df.to_id == pair["to_id"])].copy()
    if sub_df.shape[0] > 1:
        it = Itinerary(
            sub_df,
            REGION,
            fare_db,
        )
        it.clean()
        it.make_legs()
        fares["from_id"].append(pair["from_id"])
        fares["to_id"].append(pair["to_id"])
        fares["fare_cost"].append(it.compute_fare())

fare_df = pandas.DataFrame(fares)
fare_df.to_csv(
    f"/home/willem/Documents/Project/TED/data/region/{REGION}/fare/{YEAR}/fare_matrix_{YEAR}_{TYPE}.csv",
    index=False,
)

# ########## Map the fare matrix to the block groups
map_fare_matrix_to_bg(
    f"../data/region/{REGION}/fare/{YEAR}/fare_matrix_{YEAR}_{TYPE}.csv",
    f"../data/region/{REGION}/fare/BG20_cluster.csv",
    f"../data/region/{REGION}/{REGION}.gpkg",
    f"../data/region/{REGION}/fare/{YEAR}/fare_matrix_{YEAR}_{TYPE}_BG20.parquet",
)
