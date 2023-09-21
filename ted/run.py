"""Methods for setting up the data and folder structure for an analysis"""
import os
import datetime
import shutil
import sys

import geopandas as gpd
from r5py import TravelTimeMatrixComputer, TransportNetwork
import yaml

from .exception import NotAMondayError

#: The number of days since Monday to count as a weekend (Saturday = 5, Sunday = 6)
WEEKEND_DELTA = 5

#: The number of days since Monday to count as a weekday (e.g. Wednesday = 2)
WEEKDAY_DELTA = 2

"""Here's the idea

- Create a run folder structure from a YAML object

"""


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
            print(f"Running {region['name']}")

            # Create a folder for it if it doesn't exist
            region_folder = os.path.join(self.base_folder, region_key)
            create_folder_safely(region_folder)

            # Read in the centroids for the region
            centroids = gpd.read_file(region["gpkg"], layer=region["centroids_layer"])

            for network_type in ["full", "adjusted"]:
                print(f"  Running {network_type} network")

                # Read in the GTFS set
                gtfs_files = []
                for filename in os.listdir(region[f"{network_type}_gtfs_folder"]):
                    gtfs_files.append(
                        os.path.join(region[f"{network_type}_gtfs_folder"], filename)
                    )

                # Build the full network
                network = TransportNetwork(osm_pbf=region["osm"], gtfs=gtfs_files)

                # Run the matrices for the specified runs
                for run_key, run in self.regions[region_key]["runs"].items():
                    # First make sure an output folder is available
                    run_folder = os.path.join(region_folder, run_key)
                    create_folder_safely(run_folder)

                    print(f"    Running {run_key}")

                    computer = TravelTimeMatrixComputer(
                        network,
                        origins=centroids,
                        destinations=centroids,
                        departure=run["start_time"],
                        departure_time_window=datetime.timedelta(
                            minutes=run["duration"]
                        ),
                        max_time=datetime.timedelta(minutes=run["max_time"]),
                        transport_modes=["WALK", "TRANSIT"],
                    )

                    # Actually compute the travel times
                    mx = computer.compute_travel_times()

                    # Dump it into a folder
                    mx.to_parquet(
                        os.path.join(run_folder, f"{network_type}_matrix.parquet")
                    )


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
