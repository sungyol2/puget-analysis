"""Methods for setting up the data and folder structure for an analysis"""
import os
import datetime
import shutil

import geopandas as gpd
from r5py import TravelTimeMatrixComputer, TransportNetwork
import yaml

from .exception import NotAMondayError

#: The number of days since Monday to count as a weekend (Saturday = 5, Sunday = 6)
WEEKEND_DELTA = 5

#: The number of days since Monday to count as a weekday (e.g. Wednesday = 2)
WEEKDAY_DELTA = 2


class Run:
    def __init__(
        self,
        id: str,
        region_config_file: str,
        week_of: datetime.date,
        gtfs,
        gpkg,
        osm,
        start_time,
        duration,
        max_time,
    ):
        self.id = id
        self.region_config_file = region_config_file
        self.week_of = week_of
        self.gtfs = gtfs
        self.gpkg = gpkg
        self.osm = osm
        self.start_time = start_time
        self.duration = duration
        self.max_time = max_time

        # Load in region configuration file
        with open(self.region_config_file) as infile:
            self.region = yaml.safe_load(infile)

        # Grab all the filenames in the filepath
        self.gtfs_files = []
        for filename in os.listdir(self.gtfs):
            self.gtfs_files.append(os.path.join(self.gtfs, filename))

        # Grab the spatial data
        self.centroids = gpd.read_file(self.gpkg, layer="bg_centroids")

    @classmethod
    def from_yaml(cls, yaml_filepath):
        with open(yaml_filepath) as infile:
            c = yaml.safe_load(infile)
        return cls(
            c["id"],
            c["region_config_file"],
            c["week_of"],
            c["gtfs"],
            c["gpkg"],
            c["osm"],
            c["start_time"],
            c["duration"],
            c["max_time"],
        )

    def generate_matrix(self):
        network = TransportNetwork(self.osm, self.gtfs_files)

        ttmc = TravelTimeMatrixComputer(
            network,
            origins=self.centroids,
            destinations=self.centroids,
        )

    def initialize_week(
        self,
        week_of: datetime.date,
        gtfs_list: list,
        root_directory=os.path.join("..", "region"),
    ):
        # Create the date folder
        # Create a gtfs subfolder
        # Populate the date folder with GTFS data
        # Generate an initial configuration file

        date_folder = os.path.join(root_directory, self.region_key, "date")

        # Analyses go by "week of" a Monday

        if week_of.weekday() != 0:
            raise NotAMondayError(
                f"Week of date must be a Monday. Provided date is a {week_of.strftime('%A')}"
            )

        # Set weekday and weekend dates (Wed and Sat typically)
        weekday = week_of + datetime.timedelta(days=WEEKDAY_DELTA)
        weekend = week_of + datetime.timedelta(days=WEEKEND_DELTA)

        # TODO: Check for holidays, see https://stackoverflow.com/questions/2394235/detecting-a-us-holiday\

        # Set up folder structure
        week_of_folder = os.path.join(date_folder, week_of.strftime("%Y-%m-%d"))

        print(week_of_folder)
        # Make the subfolder
        os.mkdir(week_of_folder)

        # Make the GTFS folder
        os.mkdir(os.path.join(week_of_folder, "gtfs"))

        # Make the two dates folder
        os.mkdir(os.path.join(week_of_folder, "weekday_am"))
        os.mkdir(os.path.join(week_of_folder, "weekday_pm"))
        os.mkdir(os.path.join(week_of_folder, "weekend"))

        for gtfs in gtfs_list:
            shutil.copy(gtfs, os.path.join(week_of_folder, "gtfs"))


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
