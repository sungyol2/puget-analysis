import os
import datetime

from ted.gtfs import *
from ted.run import *

#: Set the region list for pulling data
REGIONS = ["BOS", "CHI", "LA", "NYC", "PHL", "SFO", "WAS"]

DATA_FOLDER = "/home/willem/Documents/Project/TED/data/"
WEEK_OF = datetime.date(2025, 3, 24)
WEDAM = datetime.datetime(2025, 3, 26, 7)
WEDPM = datetime.datetime(2025, 3, 26, 21)
SATAM = datetime.datetime(2025, 3, 29, 10)

BASE_YAML = os.path.join(DATA_FOLDER, "runs", "BASE-ALL.yaml")

# for region in REGIONS:
#     print("Preparing GTFS Data for", region)
#     base_gtfs_folder = os.path.join(
#         DATA_FOLDER, "region", region, "gtfs", "base", WEEK_OF.strftime("%Y-%m-%d")
#     )
#     full_gtfs_folder = os.path.join(
#         DATA_FOLDER, "region", region, "gtfs", "full", WEEK_OF.strftime("%Y-%m-%d")
#     )
#     limited_gtfs_folder = os.path.join(
#         DATA_FOLDER,
#         "region",
#         region,
#         "gtfs",
#         "limited",
#         WEEK_OF.strftime("%Y-%m-%d") + "-limited",
#     )
#     # Start by downloading the GTFS
#     download_gtfs_using_yaml(
#         os.path.join(DATA_FOLDER, "region", region, f"{region}.yaml"),
#         base_gtfs_folder,
#         os.path.join(
#             DATA_FOLDER, "region", region, "gtfs", f"{WEEK_OF}_download_results.csv"
#         ),
#     )

#     # Now we extend the dates as needed
#     extend_calendar_dates_and_simplify(
#         base_gtfs_folder,
#         full_gtfs_folder,
#         WEEK_OF,
#         6,
#     )

#     # Now we create a limited version
#     remove_premium_routes_from_gtfs(
#         base_gtfs_folder,
#         limited_gtfs_folder,
#         os.path.join(
#             DATA_FOLDER, "region", region, "fare", f"{region}_premium_routes.csv"
#         ),
#     )

# We have what we need now - let's build ourselves a run file
print("Creating a run YAML")
create_run_yaml(
    REGIONS,
    BASE_YAML,
    os.path.join(DATA_FOLDER, "results"),
    os.path.join(DATA_FOLDER, "runs"),
    WEEK_OF,
    WEDAM,
    WEDPM,
    SATAM,
    full_matrix=True,
    limited_matrix=True,
    tsi=True,
    access=True,
    equity=True,
)
