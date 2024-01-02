"""
Script to manage and generate fare itineraries and to compute fares

This script is a colleciton of data calls that are used to compute fare
itineraries and to compute fare matrices.

"""

import os

import geopandas

from ted.fare import *

DATA_FOLDER = "/home/willem/Documents/Project/TED/data/"

############################
#       FARE ANALYSIS      #
############################

# UTIL: Rename zipfiles with gtfs tag
add_gtfs_tag_to_zipfiles("../otp/WAS-2023-09-25-limited")

########### Load the cluster file
clusters = geopandas.read_file(
    os.path.join(DATA_FOLDER, "region", "WAS", "WAS.gpkg"), layer="clusters"
)

############## Run R5 on the eixsting clusters to get a proper list
run_r5_on_clusters(
    clusters=clusters,
    gtfs_folder=os.path.join(
        DATA_FOLDER, "region", "WAS", "gtfs", "limited", "2020-02-24-limited"
    ),
    osm_file=os.path.join(DATA_FOLDER, "region", "WAS", "WAS.osm.pbf"),
    departure=datetime.datetime(2020, 2, 26, 8),
    output_file=os.path.join(
        DATA_FOLDER, "region", "WAS", "fare", "2020", "cluster_pairs_2020_limited.csv"
    ),
)


########## Run OTP on cluster pair list

pairs = pandas.read_csv(
    os.path.join(
        DATA_FOLDER, "region", "WAS", "fare", "2020", "cluster_pairs_2020_full.csv"
    )
)

chunk_folder = "../chunks/WAS"

# Clear out what's there
for f in os.listdir(chunk_folder):
    os.remove(os.path.join(chunk_folder, f))

run_otp_itineraries_from_pairs_list(
    fares_yaml="/home/willem/Documents/Project/TED/otp/WAS-2020-02-24-full/config.yaml",
    pairs_df=pairs,
    clusters=clusters[["CLUSTER_ID", "MEAN_X", "MEAN_Y"]],
    departure=datetime.datetime(2020, 2, 26, 8),
    output_folder=chunk_folder,
    region_key="WAS",
    chunk_size=50,
)

############### Turn the generated chunks into a single itinerary set
dechunkify(
    "../chunks/WAS", "../data/region/WAS/fare/2023/itineraries_2020_limited.parquet"
)

############ Compute a fare matrix from the fares
df = pandas.read_parquet(
    os.path.join(
        DATA_FOLDER, "region", "WAS", "fare", "2020", "itineraries_2020_full.parquet"
    )
).rename(columns={"mode": "transport_mode"})

pairs = df.drop_duplicates(subset=["from_id", "to_id"])

fares = {"from_id": [], "to_id": [], "fare_cost": []}

print("Making Fare Matrix")
for idx, pair in tqdm(pairs.iterrows(), total=pairs.shape[0]):
    sub_df = df[(df.from_id == pair["from_id"]) & (df.to_id == pair["to_id"])].copy()
    if sub_df.shape[0] > 1:
        it = Itinerary(
            sub_df,
            "WAS",
            "/home/willem/Documents/Project/TED/data/region/WAS/fare/2020/WAS20.db",
        )
        it.clean()
        it.make_legs()
        fares["from_id"].append(pair["from_id"])
        fares["to_id"].append(pair["to_id"])
        fares["fare_cost"].append(it.compute_fare())

fare_df = pandas.DataFrame(fares)
fare_df.to_csv(
    "/home/willem/Documents/Project/TED/data/region/WAS/fare/2020/fare_matrix_2020_full.csv",
    index=False,
)


########### Map the fare matrix to the block groups
map_fare_matrix_to_bg(
    "../data/region/WAS/fare/2020/fare_matrix_2020_full.csv",
    "../data/region/WAS/fare/BG20_cluster.csv",
    "../data/region/WAS/WAS.gpkg",
    "../data/region/WAS/fare/2020/fare_matrix_2020_full_BG20.parquet",
)
