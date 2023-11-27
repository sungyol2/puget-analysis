"""Methods for setting up the data and folder structure for an analysis"""
import os
import datetime
import shutil
import sys

import geopandas as gpd
import pandas
from pygris import block_groups
from r5py import TravelTimeMatrixComputer, TransportNetwork
import yaml

from gtfslite import GTFS
import traccess

from .exception import NotAMondayError
from .gtfs import get_all_stops

#: The number of days since Monday to count as a weekend (Saturday = 5, Sunday = 6)
WEEKEND_DELTA = 5
#: The number of days since Monday to count as a weekday (e.g. Wednesday = 2)
WEEKDAY_DELTA = 2
#: The name of the block group column
BGNAME = "BG20"
#: The tag we use for limited sets
LIMITED_TAG = "limited"
#: Size of the Transit Service Intensity buffer to use (meters)
TSI_BUFFER_SIZE = 402.336


class Run:
    def __init__(
        self,
        run_id: str,
        description: str,
        output_folder: str,
        week_of: datetime.date,
        regions: dict,
    ):
        self.run_id = run_id
        self.description = description
        self.output_folder = output_folder
        self.week_of = week_of
        self.regions = regions

        self.base_folder = os.path.join(self.output_folder, self.run_id)
        # Create the run folder if it doesn't exist
        create_folder_safely(self.base_folder)

    @classmethod
    def from_yaml(cls, yaml_filepath):
        with open(yaml_filepath) as infile:
            c = yaml.safe_load(infile)
        return cls(
            run_id=c["run_id"],
            description=c["description"],
            output_folder=c["output_folder"],
            week_of=c["week_of"],
            regions=c["regions"],
        )

    def run_regions(self):
        """Run all regions and runs for the specified analysis."""
        for region_key, region in self.regions.items():
            with open(region["config"]) as infile:
                region_config = yaml.safe_load(infile)

            print(f"Running {region_config['name']} for {self.week_of}")

            # Create a folder for the region if it doesn't exist
            region_folder = os.path.join(self.base_folder, region_key)
            create_folder_safely(region_folder)

            # First let's open up the region configuration
            with open(region["config"]) as infile:
                region_config = yaml.safe_load(infile)

            if region["full_matrix"]:
                # Read in the centroids for the region
                centroids = gpd.read_file(
                    region_config["gpkg"], layer=region_config["centroids_layer"]
                )
                centroids.rename(columns={BGNAME: "id"}, inplace=True)
                print(f"  Running full network")
                gtfs_folder = os.path.join(region_config["gtfs"], "full", self.week_of)
                self.run_matrix(
                    region_config,
                    centroids,
                    gtfs_folder,
                    region_folder,
                    region["runs"],
                    "full_matrix",
                )
            if region["limited_matrix"]:
                centroids = gpd.read_file(
                    region_config["gpkg"], layer=region_config["centroids_layer"]
                )
                centroids.rename(columns={BGNAME: "id"}, inplace=True)
                print(f"  Running limited network")
                gtfs_folder = os.path.join(
                    region_config["gtfs"], LIMITED_TAG, f"{self.week_of}-{LIMITED_TAG}"
                )
                self.run_matrix(
                    region_config,
                    centroids,
                    gtfs_folder,
                    region_folder,
                    region["runs"],
                    f"{LIMITED_TAG}_matrix",
                )
            if region["tsi"]:
                print("Computing Transit Service Intensity")
                # Need to get the shapes
                areas = gpd.read_file(
                    region_config["gpkg"], layer=region_config["areas_layer"]
                )
                areas.geometry = areas.geometry.buffer(TSI_BUFFER_SIZE)
                runs = []
                for run_key, run in region["runs"].items():
                    areas[run_key] = 0
                    runs.append(run_key)
                # Now we get the stops in the region
                gtfs_folder = os.path.join(region_config["gtfs"], "full", self.week_of)
                all_stops = get_all_stops(gtfs_folder).to_crs(areas.crs)
                print("Starting TSI computation")
                for agency in all_stops.agency.unique():
                    print("  Computing for", agency)
                    # Load the zipfile
                    agency_stops = all_stops[all_stops.agency == agency].copy()
                    gtfs = GTFS.load_zip(os.path.join(gtfs_folder, f"{agency}.zip"))
                    # Compute the spatial intersection
                    joined = gpd.sjoin(
                        left_df=agency_stops[["stop_id", "geometry"]],
                        right_df=areas[[BGNAME, "geometry"]],
                    )
                    # Let's get the TSI
                    for bg in joined.BG20.unique():
                        # print("  Checking", bg)
                        stops = joined[joined.BG20 == bg]["stop_id"].tolist()
                        # print("  Found", len(stops), "stops.")
                        for run_key, run in region["runs"].items():
                            # print("    Checking on", run_key)
                            start_time = run
                            end_time = start_time + datetime.timedelta(hours=2)
                            tsi = gtfs.unique_trip_count_at_stops(
                                stops,
                                date=start_time.date(),
                                start_time=start_time.strftime("%H:%M:%S"),
                                end_time=end_time.strftime("%H:%M:%S"),
                            )
                            areas.loc[areas.BG20 == bg, run_key] += tsi
                # Finish off by joining in items
                runs.append(BGNAME)
                out = areas[runs].set_index(BGNAME)
                out.to_csv(os.path.join(region_folder, "tsi.csv"))
                #     df.to_csv(os.path.join(run_folder, "tsi.csv"), index=False)
            if region["access"]:
                print("Computing access metrics")
                # Compute access metrics
                supply = traccess.Supply.from_csv(
                    region_config["supply"], dtype={"BG20": str}, id_column="BG20"
                )
                for run_key, run in region["runs"].items():
                    run_folder = os.path.join(region_folder, run_key)
                    # Let's do full matrix first
                    full_cost = traccess.Cost.from_parquet(
                        os.path.join(run_folder, "full_matrix.parquet"),
                        from_id="from_id",
                        to_id="to_id",
                    )
                    # Now let's compute some STUFF
                    ac = traccess.AccessComputer(supply, full_cost)
                    print(f"    {run_key}: Computing c15 measures")
                    c15 = ac.cumulative_cutoff(
                        cost_columns=["travel_time"],
                        cutoffs=[15],
                        supply_columns=["acres"],
                    ).data
                    c15.columns = ["acres_c15"]

                    print(f"    {run_key}: Computing c30 measures")
                    c30 = ac.cumulative_cutoff(
                        cost_columns=["travel_time"],
                        cutoffs=[30],
                        supply_columns=["C000", "acres"],
                    ).data
                    c30.columns = ["C000_c30", "acres_c30"]

                    print(f"    {run_key}: Computing c45 measures")
                    c45 = ac.cumulative_cutoff(
                        cost_columns=["travel_time"],
                        cutoffs=[45],
                        supply_columns=["C000"],
                    ).data
                    c45.columns = ["C000_c45"]

                    print(f"    {run_key}: Computing c60 measures")
                    c60 = ac.cumulative_cutoff(
                        cost_columns=["travel_time"],
                        cutoffs=[60],
                        supply_columns=["C000"],
                    ).data
                    c60.columns = ["C000_c60"]

                    print(f"    {run_key}: Computing t1 measures")
                    t1 = ac.cost_to_closest(
                        "travel_time",
                        [
                            "education",
                            "grocery",
                            "hospitals",
                            "pharmacies",
                            "urgent_care_facilities",
                        ],
                        n=1,
                    ).data
                    t1.columns = {f"{c}_t1" for c in t1.columns}

                    print(f"    {run_key}: Computing t3 measures")
                    t3 = ac.cost_to_closest(
                        "travel_time",
                        [
                            "education",
                            "grocery",
                            "hospitals",
                            "pharmacies",
                            "urgent_care_facilities",
                        ],
                        n=3,
                    ).data
                    t3.columns = {f"{c}_t3" for c in t3.columns}

                    df = c15.join(c30)
                    df = df.join(c45)
                    df = df.join(c60)
                    df = df.join(t1)
                    df = df.join(t3)
                    df = df.reset_index().rename(columns={"from_id": "BG20"})
                    df.to_csv(os.path.join(run_folder, "access.csv"), index=False)

            if region["equity"]:
                # Compute equity summaries
                pass

    def run_matrix(
        self, region, centroids, gtfs_folder, region_folder, runs, output_name
    ):
        gtfs_files = []
        for filename in os.listdir(gtfs_folder):
            if (not filename.startswith(".")) and (filename.endswith(".zip")):
                gtfs_files.append(os.path.join(gtfs_folder, filename))

        # Build the full network
        print("   building transport network")
        network = TransportNetwork(osm_pbf=region["osm"], gtfs=gtfs_files)

        # Run the matrices for the specified runs
        for run_key, run in runs.items():
            # First make sure an output folder is available
            run_folder = os.path.join(region_folder, run_key)
            create_folder_safely(run_folder)

            print(f"    Running {run_key}")

            computer = TravelTimeMatrixComputer(
                network,
                origins=centroids,
                destinations=centroids,
                departure=run,
                departure_time_window=datetime.timedelta(minutes=120),
                max_time=datetime.timedelta(minutes=180),
                transport_modes=["WALK", "TRANSIT"],
            )

            # Actually compute the travel times
            mx = computer.compute_travel_times()
            # Dump it into a folder
            mx.to_parquet(os.path.join(run_folder, f"{output_name}.parquet"))


def create_folder_safely(folder_path: os.path):
    """Create a folder if it doesn't exist

    Parameters
    ----------
    folder_path : os.path
        The path to the folder to create or check for existence
    """
    if not os.path.exists(folder_path):
        os.mkdir(folder_path)


def create_regions(root_directory):
    for region in ["BOS", "WAS", "CHI", "PHI", "LA", "SFO", "NYC"]:
        region_path = os.path.join(root_directory, region)
        if not os.path.exists(region_path):
            os.mkdir(region_path)

        # Create a subfolder for static data
        static_path = os.path.join(region_path, "static")
        os.mkdir(static_path)

        # Create a subfolder for fare-specific data
        fare_path = os.path.join(region_path, "fare")
        os.mkdir(fare_path)

        # Create a subfolder for date-specific analyses
        date_path = os.path.join(region_path, "date")
        os.mkdir(os.path.join(date_path))


def create_run_yamls_from_csv(
    region_key,
    run_catalog_path,
    template_yaml_path,
    results_folder,
    runs_folder,
    full_matrix: bool = False,
    limited_matrix: bool = False,
    tsi: bool = False,
    access: bool = False,
    equity: bool = False,
):
    run_catalog = pandas.read_csv(run_catalog_path)
    with open(template_yaml_path) as infile:
        config = yaml.safe_load(infile)
    for idx, run in run_catalog.iterrows():
        config["week_of"] = run["week_of"]
        config["run_id"] = f"{run['week_of']}-{region_key}"
        config["regions"][region_key]["access"] = access
        config["regions"][region_key]["equity"] = equity
        # Let's check if the full matrix exists
        if not os.path.exists(
            os.path.join(
                results_folder,
                config["run_id"],
                region_key,
                "SATAM",
                "full_matrix.parquet",
            )
        ):
            config["regions"][region_key]["full_matrix"] = full_matrix
        else:
            print("Already a full matrix")
            config["regions"][region_key]["full_matrix"] = full_matrix
        config["regions"][region_key]["limited_matrix"] = limited_matrix
        config["regions"][region_key]["tsi"] = tsi
        config["regions"][region_key]["runs"]["SATAM"] = datetime.datetime.strptime(
            run["SATAM"], "%Y-%m-%d %H:%M:%S"
        )
        config["regions"][region_key]["runs"]["WEDAM"] = datetime.datetime.strptime(
            run["WEDAM"], "%Y-%m-%d %H:%M:%S"
        )
        config["regions"][region_key]["runs"]["WEDPM"] = datetime.datetime.strptime(
            run["WEDPM"], "%Y-%m-%d %H:%M:%S"
        )

        config["description"] = f"Analysis for {region_key} on {run['week_of']}"
        outname = f"{run['week_of']}-{region_key}-MX-TSI.yaml"
        with open(os.path.join(runs_folder, region_key, outname), "w") as outfile:
            yaml.dump(config, outfile)
