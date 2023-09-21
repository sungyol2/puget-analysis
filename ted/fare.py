import datetime
import os

import geopandas as gpd
import yaml

import r5py

class ItinerariesGenerator:
    def __init__(
        self,
        run_id: str,
        description: str,
        output_folder: str,
        week_of: datetime.date,
        osm: str,
        gpkg: str,
        gtfs: str,
        centroids_layer: str,
        start_time: datetime.datetime,
        duration: int,
        max_time: int
    ):
        self.run_id = run_id
        self.description = description
        self.output_folder = output_folder
        self.week_of = week_of
        self.osm = osm
        self.gpkg = gpkg
        self.gtfs = gtfs
        self.centroids_layer = centroids_layer
        self.start_time = start_time
        self.duration = duration
        self.max_time = max_time


    def generate_itineraries(self, sample=0):
        # Okay let's do this
        centroids = gpd.read_file(self.gpkg, layer=self.centroids_layer)
        if sample > 0:
            centroids = centroids.sample(sample).copy()

        # Read in the GTFS set
        gtfs_files = []
        for filename in os.listdir(self.gtfs):
            gtfs_files.append(
                os.path.join(self.gtfs, filename)
            )

        # Build the full network
        network = r5py.TransportNetwork(osm_pbf=self.osm, gtfs=gtfs_files)
        
        computer = r5py.DetailedItinerariesComputer(
            network,
            origins=centroids,
            departure=self.start_time,
            transport_modes=[r5py.TransportMode.TRANSIT, r5py.TransportMode.WALK],
            snap_to_network=True
        )

        print(centroids)
        travel_details = computer.compute_travel_details()

        travel_details.drop(columns=["geometry"], inplace=True)

        print(travel_details)
        print(travel_details.dtypes)

        # Convert the object columns into something more parseable
        travel_details["transport_mode"] = travel_details["transport_mode"].astype(str)
        travel_details["travel_time"] = travel_details.travel_time.apply(
            lambda t: round(t.total_seconds() / 60.0, 2)
        )
        travel_details["wait_time"] = travel_details.wait_time.apply(
            lambda t: round(t.total_seconds() / 60.0, 2)
        )

        print(travel_details)
        print(travel_details.dtypes)

        # Dump it into a folder
        travel_details.to_parquet(
            os.path.join(self.output_folder, f"{self.run_id}_details.parquet")
        )

    @classmethod
    def from_yaml(cls, yaml_filepath):
        with open(yaml_filepath) as infile:
            c = yaml.safe_load(infile)

        return cls(
            run_id=c["run_id"],
            description=c["description"],
            output_folder=c["output_folder"],
            week_of=c["week_of"],
            osm=c["osm"],
            gpkg=c["gpkg"],
            gtfs=c["gtfs"],
            centroids_layer=c["centroids_layer"],
            start_time=c["start_time"],
            duration=c["duration"],
            max_time=c["max_time"]
        )
    

class Itinerary:

    def __init__(self, json):
        pass

    def compute_fare(self) -> float:
        pass