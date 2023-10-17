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

from .exception import NotAMondayError
from .gtfs import get_all_stops

#: The number of days since Monday to count as a weekend (Saturday = 5, Sunday = 6)
WEEKEND_DELTA = 5
#: The number of days since Monday to count as a weekday (e.g. Wednesday = 2)
WEEKDAY_DELTA = 2
#: The name of the block group column
BGNAME = "BG20"
#: The tag we use for limited sets
LIMITED_TAG = "-limited"
#: Size of the Transit Service Intensity buffer to use (meters)
TSI_BUFFER_SIZE = 200


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

            print(f"Running {region_config['name']}")

            # Create a folder for the region if it doesn't exist
            region_folder = os.path.join(self.base_folder, region_key)
            create_folder_safely(region_folder)

            # First let's open up the region configuration
            with open(region["config"]) as infile:
                region_config = yaml.safe_load(infile)

            if region["full_matrix"]:
                # Read in the centroids for the region
                centroids = gpd.read_file(region["gpkg"], layer=region["centroids_layer"])
                centroids.rename(columns={BGNAME, "id"})
                print(f"  Running full network")
                gtfs_folder = os.path.join(region_config["gtfs"], self.week_of)
                self.run_matrix(region_config, centroids, gtfs_folder, region_folder, region["runs"], "full_matrix")
            if region["limited_matrix"]:
                centroids = gpd.read_file(region["gpkg"], layer=region["centroids_layer"])
                centroids.rename(columns={BGNAME, "id"})
                print(f"  Running full network")
                gtfs_folder = os.path.join(region_config["gtfs"], f"self.week_of-{LIMITED_TAG}")
                self.run_matrix(region_config, centroids, gtfs_folder, region_folder, region["runs"], "limited_matrix")
            if region["tsi"]:
                # Need to get the shapes
                areas = gpd.read_file(region_config["gpkg"], layer=region_config["areas_layer"])
                areas.geometry = areas.geometry.buffer(TSI_BUFFER_SIZE)
                # Now we load the stops for each region
                gtfs_folder = os.path.join(region_config["gtfs"], self.week_of)
                print("Computing Transit Service Intensity")
                all_stops = get_all_stops(gtfs_folder).to_crs(areas.crs)
                for run_key, run in region["runs"].items():
                    print("  Calculating TSI for", run_key)
                    start_time = run
                    end_time = start_time + datetime.timedelta(hours=2)
                    run_folder = os.path.join(region_folder, run_key)
                    create_folder_safely(run_folder)
                    results = {BGNAME: [], "run_key": [], "tsi": []}
                    count = 1
                    total = areas.shape[0]
                    for idx, bg in areas.iterrows():
                        if count % 100 == 0:
                            print(f"  {100 * count/total} percent complete.")
                        count += 1
                        subset = all_stops[all_stops.within(bg.geometry)]
                        agency_list = subset["agency"].unique()
                        unique_trips = 0
                        for agency in agency_list:
                            gtfs = GTFS.load_zip(os.path.join(gtfs_folder, f"{agency}.zip"))
                            unique_trips += gtfs.unique_trip_count_at_stops(
                                stop_ids=subset[subset.agency == agency].stop_id.tolist(),
                                date=start_time.date(),
                                start_time=start_time.strftime("%H:%M:%S"),
                                end_time=end_time.strftime("%H:%M:%S"),
                            )
                        results[BGNAME].append(bg[BGNAME])
                        results["run_key"].append(run_key)
                        results["tsi"].append(unique_trips / 2)

                    df = pandas.DataFrame(results)
                    df.to_csv(os.path.join(run_folder, "tsi.csv"), index=False)

    def run_matrix(region, centroids, gtfs_folder, region_folder, runs, output_name):
        gtfs_files = []
        for filename in os.listdir(gtfs_folder):
            gtfs_files.append(os.path.join(gtfs_folder, filename))

        # Build the full network
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
                departure=run["start_time"],
                departure_time_window=datetime.timedelta(minutes=run["duration"]),
                max_time=datetime.timedelta(minutes=run["max_time"]),
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


def create_run_yamls_from_csv(csv_file, run_folder, output_folder, duration=120, max_time=180):
    runs = pandas.read_csv(csv_file)
    runs["WEDAM"] = pandas.to_datetime(runs.WEDAM)
    runs["WEDPM"] = pandas.to_datetime(runs.WEDPM)
    runs["SATAM"] = pandas.to_datetime(runs.SATAM)

    # Now we go by week
    for week_of in runs["week_of"].unique():
        wk_df = runs[runs["week_of"] == week_of].copy()
        run_id = f"{week_of}"
        run_dict = dict(
            {
                "run_id": run_id,
                "description": "Main Data Run",
                "output_folder": output_folder,
                "week_of": week_of,
                "regions": {},
            }
        )
        for region in wk_df.region.uinque():
            run_dict["regions"][region] = {"name": None}
            rg_df = wk_df[wk_df.region == region].copy()
            for idx, row in rg_df.iterrows():
                filename = f"{week_of}.yaml"
                print(run_id)
