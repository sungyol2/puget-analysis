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

############################
#       FARE ANALYSIS      #
############################

# UTIL: Rename zipfiles with gtfs tag
# add_gtfs_tag_to_zipfiles("../otp/SFO-2023-09-25-full")

# UTIL: Port data over from another folder and name it for OTP's sake
# otp_folder = "/home/willem/Documents/Project/TED/otp/SFO-2023-09-25-full"
# gtfs_folder = os.path.join(DATA_FOLDER, "region", "SFO", "gtfs", "full", "2023-09-25")
# for f in os.listdir(gtfs_folder):
#     print(f)
#     z = GTFS.load_zip(os.path.join(gtfs_folder, f), ignore_optional_files="keep_shapes")
#     if "stop_timezone" in z.stops.columns:
#         z.stops = z.stops.drop(columns=["stop_timezone"])
#     z.stop_times["arrival_time"] = z.stop_times["arrival_time"].str.replace("nan", "")
#     z.stop_times["departure_time"] = z.stop_times["departure_time"].str.replace(
#         "nan", ""
#     )
#     z.write_zip(os.path.join(otp_folder, "gtfs-" + f))

########### Load the cluster file
clusters = geopandas.read_file(
    os.path.join(DATA_FOLDER, "region", "SFO", "SFO.gpkg"), layer="clusters"
)

############## Run R5 on the eixsting clusters to get a proper list
# run_r5_on_clusters(
#     clusters=clusters,
#     gtfs_folder=os.path.join(
#         DATA_FOLDER, "region", "SFO", "gtfs", "full", "2023-09-25"
#     ),
#     osm_file=os.path.join(DATA_FOLDER, "region", "SFO", "SFO.osm.pbf"),
#     departure=datetime.datetime(2023, 9, 27, 8),
#     output_file=os.path.join(
#         DATA_FOLDER, "region", "SFO", "fare", "2023", "cluster_pairs_2023_full.csv"
#     ),
# )


########## Run OTP on cluster pair list

# pairs = pandas.read_csv(
#     os.path.join(
#         DATA_FOLDER, "region", "SFO", "fare", "2023", "cluster_pairs_2023_full.csv"
#     )
# )

# chunk_folder = "../chunks/SFO"

# for f in os.listdir(chunk_folder):
#     os.remove(os.path.join(chunk_folder, f))

# run_otp_itineraries_from_pairs_list(
#     fares_yaml="/home/willem/Documents/Project/TED/otp/SFO-2023-09-25-full/config.yaml",
#     pairs_df=pairs,
#     clusters=clusters[["CLUSTER_ID", "MEAN_X", "MEAN_Y"]],
#     departure=datetime.datetime(2023, 9, 25, 8),
#     output_folder=chunk_folder,
#     region_key="SFO",
#     chunk_size=50,
# )

############### Turn the generated chunks into a single itinerary set
# dechunkify(
#     "../chunks/SFO", "../data/region/SFO/fare/2023/itineraries_2023_full.parquet"
# )

########## Get unique routes or municipalities used
# get_unique_routes_used(
#     os.path.join(
#         DATA_FOLDER, "region", "SFO", "fare", "2023", "itineraries_2023_full.parquet"
#     ),
#     os.path.join(DATA_FOLDER, "region", "SFO", "fare", "unique_routes.csv"),
# )

############ Compute a fare matrix from the fares
# df = pandas.read_parquet(
#     os.path.join(
#         DATA_FOLDER, "region", "SFO", "fare", "2020", "itineraries_2020_full.parquet"
#     )
# ).rename(columns={"mode": "transport_mode"})

# pairs = df.drop_duplicates(subset=["from_id", "to_id"])

# fares = {"from_id": [], "to_id": [], "fare_cost": []}

# print("Making Fare Matrix")
# for idx, pair in tqdm(pairs.iterrows(), total=pairs.shape[0]):
#     sub_df = df[(df.from_id == pair["from_id"]) & (df.to_id == pair["to_id"])].copy()
#     if sub_df.shape[0] > 1:
#         it = Itinerary(
#             sub_df,
#             "WAS",
#             "/home/willem/Documents/Project/TED/data/region/WAS/fare/2020/WAS20.db",
#         )
#         it.clean()
#         it.make_legs()
#         fares["from_id"].append(pair["from_id"])
#         fares["to_id"].append(pair["to_id"])
#         fares["fare_cost"].append(it.compute_fare())

# fare_df = pandas.DataFrame(fares)
# fare_df.to_csv(
#     "/home/willem/Documents/Project/TED/data/region/WAS/fare/2020/fare_matrix_2020_full.csv",
#     index=False,
# )


########### Map the fare matrix to the block groups
# map_fare_matrix_to_bg(
#     "../data/region/WAS/fare/2020/fare_matrix_2020_full.csv",
#     "../data/region/WAS/fare/BG20_cluster.csv",
#     "../data/region/WAS/WAS.gpkg",
#     "../data/region/WAS/fare/2020/fare_matrix_2020_full_BG20.parquet",
# )
