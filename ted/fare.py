import datetime
import logging
import os

import geopandas as gpd
import numpy
import pandas
import yaml

import r5py

logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.INFO)

INFINITE_INT = 100000


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
        max_time: int,
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
        logging.info("Initializing itinerary generation")
        centroids = gpd.read_file(self.gpkg, layer=self.centroids_layer)
        if sample > 0:
            centroids = centroids.sample(sample).copy()

        # Read in the GTFS set
        gtfs_files = []
        for filename in os.listdir(self.gtfs):
            gtfs_files.append(os.path.join(self.gtfs, filename))

        # Build the full network
        network = r5py.TransportNetwork(osm_pbf=self.osm, gtfs=gtfs_files)
        logging.info("Travel built, computing travel details")
        computer = r5py.DetailedItinerariesComputer(
            network,
            origins=centroids,
            departure=self.start_time,
            max_time=datetime.timedelta(minutes=self.max_time),
            max_time_walking=datetime.timedelta(minutes=30),
            transport_modes=[r5py.TransportMode.TRANSIT, r5py.TransportMode.WALK],
            snap_to_network=True,
        )
        travel_details = computer.compute_travel_details()

        travel_details.drop(columns=["geometry"], inplace=True)

        # Convert the object columns into something more parseable
        travel_details["transport_mode"] = travel_details["transport_mode"].astype(str)
        travel_details.replace(to_replace=[None], value=numpy.nan, inplace=True)
        travel_details["travel_time"] = pandas.to_timedelta(travel_details.travel_time)
        travel_details["wait_time"] = pandas.to_timedelta(travel_details.wait_time)
        travel_details["travel_time"] = travel_details.travel_time.apply(lambda t: round(t.total_seconds() / 60.0, 2))
        travel_details["wait_time"] = travel_details.wait_time.apply(lambda t: round(t.total_seconds() / 60.0, 2))

        logging.debug(travel_details)
        logging.debug(travel_details.dtypes)

        # Dump it into a folder
        travel_details.to_parquet(os.path.join(self.output_folder, f"{self.run_id}_details.parquet"))

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
            max_time=c["max_time"],
        )


class Itinerary:
    def __init__(self, itinerary_df: pandas.DataFrame, region: str):
        self._df = itinerary_df.sort_values("segment")
        self.region = region
        self._df.departure_time = pandas.to_datetime(self._df.departure_time)
        self._legs = []

    def clean(self):
        # Check that the first row is "walking"
        if self._df.iloc[0].transport_mode == "WALK":
            self._df = self._df.tail(-1)
        if self._df.iloc[-1].transport_mode == "WALK":
            self._df = self._df.head(-1)

    def make_legs(self):
        prev_leg = None
        for idx, row in self._df.iterrows():
            if row["transport_mode"] != "WALK":
                this_leg = TransitLeg.from_row(row, prev_leg)
                self._legs.append(this_leg)
                prev_leg = this_leg

    def print_legs(self):
        leg = self._legs[0]
        print(leg)
        while leg.next_leg is not None:
            # Update the leg and print it
            leg = leg.next_leg
            print(leg)

    def compute_fare(self) -> int:
        pass


class TransitLeg:
    def __init__(
        self, transport_mode, departure_time, feed, agency_id, route_id, start_stop_id, end_stop_id, prev_leg, next_leg
    ):
        self.transport_mode = transport_mode
        self.departure_time = departure_time
        self.feed = feed
        self.agency_id = agency_id
        self.route_id = route_id
        self.start_stop_id = start_stop_id
        self.end_stop_id = end_stop_id
        self.prev_leg = prev_leg
        self.next_leg = next_leg

    def __repr__(self) -> str:
        return f"<TransitLeg {self.transport_mode} {self.departure_time} | {self.route_id}:{self.start_stop_id}->{self.end_stop_id}>"

    @classmethod
    def from_row(cls, r, prev_leg):
        leg = cls(
            r.transport_mode,
            r.departure_time,
            r.feed,
            r.agency_id,
            r.route_id,
            r.start_stop_id,
            r.end_stop_id,
            prev_leg,
            None,
        )
        # Link the list
        if prev_leg is not None:
            leg.prev_leg.next_leg = leg

        return leg


class BaseFare:
    def __init__(self, start_time, duration, transfers=-1):
        self.start_time = start_time
        self.active = True

        if duration > 0:
            self.max_time = duration
        else:
            self.max_time = INFINITE_INT

        if transfers >= 0:
            self.transfers = transfers
        else:
            self.transfers = INFINITE_INT

    def is_time_valid(self, datetime_to_check: datetime.datetime):
        if ((datetime_to_check - self.start_time).total_seconds()) > self.max_time:
            return False
        else:
            return True


class FixedFare(BaseFare):
    def __init__(self, start_time, max_time, max_transfers):
        super().__init__(start_time, max_time, max_transfers)


class ZoneFare(BaseFare):
    def __init__(self, start_time, max_time, max_transfers, start_zone, end_zone):
        super().__init__(start_time, max_time, max_transfers)
