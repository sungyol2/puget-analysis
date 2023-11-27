import datetime
import json
import logging
import multiprocessing
import os
import requests
from pytz import timezone
import time

import geopandas as gpd
import numpy
import pandas
from tqdm import tqdm
import sqlite3
import yaml


import r5py

from .exception import NoExistingFareError

logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.INFO)

INFINITE_INT = 100000
MAX_FARE_TRAVEL_TIME = 180
WALK_MODE = "TransportMode.WALK"
DB = "/home/willem/Documents/Project/TED/data/region/WAS/fare/WAS.db"

TRANSFER_DISCOUNT = "transfer-discount"


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
        print("Initializing itinerary generation")
        centroids = gpd.read_file(self.gpkg, layer=self.centroids_layer)
        centroids = centroids.rename(columns={"TR20": "id"})
        if sample > 0:
            centroids = centroids.sample(sample).copy()

        num_centroids = centroids.shape[0]
        print(
            f"  This itinerary matrix will have size {num_centroids} x {num_centroids} = {num_centroids*num_centroids}"
        )

        # Read in the GTFS set
        gtfs_files = []
        for filename in os.listdir(self.gtfs):
            gtfs_files.append(os.path.join(self.gtfs, filename))

        # Build the full network
        print("  Building network")
        network = r5py.TransportNetwork(osm_pbf=self.osm, gtfs=gtfs_files)
        print("  Network built, computing travel details")
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
        travel_details["travel_time"] = travel_details.travel_time.apply(
            lambda t: round(t.total_seconds() / 60.0, 2)
        )
        travel_details["wait_time"] = travel_details.wait_time.apply(
            lambda t: round(t.total_seconds() / 60.0, 2)
        )

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
            max_time=c["max_time"],
        )


class ItineraryCollection:
    def __init__(self, itineraries_df: pandas.DataFrame, region: str):
        self.region = region
        print("Loaded itineraries, getting fastest options")
        itineraries_df = itineraries_df[itineraries_df.from_id != itineraries_df.to_id]
        fastest = itineraries_df[
            ["from_id", "to_id", "option", "wait_time", "travel_time"]
        ].fillna(0)
        fastest["total_time"] = fastest["wait_time"] + fastest["travel_time"]
        fastest = (
            fastest[["from_id", "to_id", "option", "total_time"]]
            .groupby(["from_id", "to_id", "option"], as_index=False)
            .sum()
        )
        fastest = fastest.sort_values(["from_id", "to_id", "total_time"])
        fastest = fastest[fastest.total_time <= MAX_FARE_TRAVEL_TIME]
        fastest.drop_duplicates(subset=["from_id", "to_id"], keep="first", inplace=True)
        fastest_only = pandas.merge(
            itineraries_df, fastest, on=["from_id", "to_id", "option"]
        )
        fastest_only.sort_values(["from_id", "to_id", "segment"])
        fastest_only["pair"] = fastest_only["from_id"] + "-" + fastest_only["to_id"]
        self._df = fastest_only
        self._itineraries = []
        for pair in fastest_only["pair"].unique():
            idf = self._df[self._df["pair"] == pair]
            it = Itinerary(idf, region)
            it.clean()
            it.make_legs()
            self._itineraries.append(it)

    @property
    def size(self) -> int:
        return len(self._itineraries)


class Itinerary:
    def __init__(self, itinerary_df: pandas.DataFrame, region: str):
        self._df = itinerary_df.sort_values("segment")
        self.region = region
        self._df.departure_time = pandas.to_datetime(self._df.departure_time)
        self._legs = []
        self._fares = []

    def clean(self):
        # Check that the first row is "walking"
        if self._df.iloc[0].transport_mode == WALK_MODE:
            self._df = self._df.tail(-1)
        if self._df.iloc[-1].transport_mode == WALK_MODE:
            self._df = self._df.head(-1)

    def make_legs(self):
        prev_leg = None
        for idx, row in self._df.iterrows():
            if row["transport_mode"] != WALK_MODE:
                this_leg = TransitLeg.from_row(row, prev_leg)
                self._legs.append(this_leg)
                prev_leg = this_leg

    def print_legs(self, with_feeds=False):
        leg = self._legs[0]
        if with_feeds:
            print(leg.feed, leg)
        else:
            print(leg)
        while leg.next_leg is not None:
            # Update the leg and print it
            leg = leg.next_leg
            if with_feeds:
                print(leg.feed, leg)
            else:
                print(leg)

    def print_fares(self, with_feeds=False):
        for fare in self._fares:
            if with_feeds == True:
                print(fare.feed, fare)
            else:
                print(fare)

    def compute_fare(self) -> int:
        # Empty existing fares
        self._fares = []
        leg = self._legs[0]
        # We know there's no fare existing for the first one
        fare = self.get_new_fare(leg)
        self._fares.append(fare)
        while leg.next_leg is not None:
            # We have ourselves a transfer
            from_leg = leg
            to_leg = leg.next_leg
            current_time = to_leg.departure_time

            # Let's go ahead and update all fares
            self.update_fare_times(current_time)

            # Let's find out if the next leg is already covered by the existing fares
            df = from_leg.transfers[from_leg.transfers.to_mdb_slug == to_leg.feed]
            # Keep only those with all routes or specified stops
            df = df[
                (
                    (df.from_route_id == from_leg.route_id)
                    | (df.from_route_id == "__ANY__")
                )
                & ((df.to_route_id == to_leg.route_id) | (df.to_route_id == "__ANY__"))
            ]
            df = df[
                (
                    (df.from_stop_id == from_leg.end_stop_id)
                    | (df.from_stop_id == "__ANY__")
                )
                & (
                    (df.to_stop_id == to_leg.start_stop_id)
                    | (df.to_stop_id == "__ANY__")
                )
            ]
            if df.shape[0] > 0:
                tfr = df.iloc[0]
                # For a transfer discount, we want to apply it
                if tfr.transfer_type == "transfer-discount":
                    # Let's apply a discount to the next route's fare
                    if tfr.new_fare == 0:
                        # Update the existing fare if exists
                        try:
                            self.update_existing_fare(
                                from_leg,
                                discount=int(tfr.fare_value),
                                transfer_decrease=1,
                            )
                        except NoExistingFareError:
                            fare = self.get_new_fare(to_leg)
                            self._fares.append(fare)
                    else:
                        fare = self.get_new_fare(to_leg)
                        fare.discount = int(tfr.fare_value)
                        self._fares.append(fare)
                else:
                    if tfr.new_fare == 0:
                        try:
                            self.update_existing_fare(
                                from_leg,
                                transfer_decrease=1,
                                cost_increase=tfr.fare_value,
                            )
                        except NoExistingFareError:
                            # We need a new fare after all
                            fare = self.get_new_fare(to_leg)
                            self._fares.append(fare)
                    else:
                        fare = self.get_new_fare(to_leg)
                        fare.cost = int(tfr.fare_value)
                        self._fares.append(fare)
            else:
                # A new fare if not within a feed and not specified in the transfers
                if from_leg.feed != to_leg.feed:
                    fare = self.get_new_fare(to_leg)
                    self._fares.append(fare)
            leg = to_leg

        total_fare = 0
        # Now we need to "close" off the fares

        for fare in self._fares:
            total_fare += fare.net_fare()
        return total_fare

    def update_fare_times(self, current_time):
        for fare in self._fares:
            elapsed = (current_time - fare.start_time).total_seconds()
            if elapsed > fare.max_time and fare.active == True:
                print("Fare time exceeded for", fare)
                fare.active = False

    def update_existing_fare(
        self, leg, transfer_decrease=0, cost_increase=0, discount=0
    ):
        for fare in self._fares:
            if fare.active == True:
                if fare.feed == leg.feed:
                    fare.transfers -= transfer_decrease
                    fare.cost += int(cost_increase)
                    fare.discount = int(discount)
                    # We did what we did
                    return
        raise NoExistingFareError

    def get_new_fare(self, leg):
        # Let's start by retrieving the fare
        sql = f"""
        SELECT fare_type.fare_type, fare_type.transfers_allowed, fare_type.fare_duration 
        FROM fare_type
        WHERE fare_type.mdb_slug = "{leg.feed}"
        """
        res = execute_sql(sql)[0]

        if res[0] == "flat":
            fare = FixedFare(leg.departure_time, res[1], res[2], leg.feed)
            rf = self.get_route_fare_cost(leg)
            if rf is not None:
                fare.cost = rf
                fare.premium = True
            else:
                # Look up the regular fare
                fare.cost = self.get_flat_fare_cost(leg)
                fare.premium = False
        else:
            start_zone, end_zone = self.get_zones_from_leg(leg)
            fare = ZoneFare(
                start_time=leg.departure_time,
                max_transfers=res[1],
                max_time=res[2],
                feed=leg.feed,
                route_id=leg.route_id,
                from_zone=start_zone,
                to_zone=end_zone,
            )
            fare.premium = True
            fare.update_fare()
        return fare

    def get_zones_from_leg(self, leg):
        sql = f"""
        SELECT zone_id
        FROM "zone"
        where "zone".mdb_slug = '{leg.feed}'
        and "zone".stop_id = '{leg.start_stop_id}'
        """
        start_zone = execute_sql(sql)[0][0]

        sql = f"""
        SELECT zone_id
        FROM "zone"
        where "zone".mdb_slug = '{leg.feed}'
        and "zone".stop_id = '{leg.end_stop_id}'
        """
        end_zone = execute_sql(sql)[0][0]

        return start_zone, end_zone

    def get_route_fare_cost(self, leg) -> int:
        """Get the route-specific fare cost, if there is one

        Parameters
        ----------
        leg : TransitLeg
            The transit leg to check

        Returns
        -------
        int
            The fare cost"""

        sql = f"""
        SELECT rf.fare_cost 
        FROM route_fare rf
        where rf.mdb_slug = '{leg.feed}'
        AND rf.route_id = '{leg.route_id}'
        """
        res = execute_sql(sql)
        if len(res) > 0:
            return int(res[0][0])
        else:
            return None

    def get_flat_fare_cost(self, leg) -> int:
        """Get the flat fare cost for the leg

        Parameters
        ----------
        leg : TransitLeg
            The transit leg to check

        Returns
        -------
        int
            The fare cost
        """

        sql = f"""
        SELECT ff.fare_cost
        FROM flat_fare ff 
        where ff.mdb_slug = "{leg.feed}"
        """

        return int(execute_sql(sql)[0][0])


def execute_sql(sql) -> list:
    conn = sqlite3.connect(DB)
    cursor = conn.cursor()
    cursor.execute(sql)
    res = cursor.fetchall()
    cursor.close()
    return res


def execute_sql_to_df(sql) -> pandas.DataFrame:
    conn = sqlite3.connect(DB)
    df = pandas.read_sql_query(sql, conn)
    return df


class TransitLeg:
    def __init__(
        self,
        transport_mode,
        departure_time,
        feed,
        agency_id,
        route_id,
        start_stop_id,
        end_stop_id,
        prev_leg,
        next_leg,
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

        # Let's get the transfers
        sql = f"""
        SELECT * from transfer
        where transfer.from_mdb_slug = '{self.feed}'
        """
        self.transfers = execute_sql_to_df(sql)

    def __repr__(self) -> str:
        return f"<TransitLeg {self.transport_mode} {self.departure_time} | {self.route_id}:{self.start_stop_id}->{self.end_stop_id}>"

    @classmethod
    def from_row(cls, r, prev_leg):
        leg = cls(
            r.transport_mode,
            r.departure_time.to_pydatetime(),
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
    def __init__(self, start_time, transfers, duration, feed):
        self.start_time = start_time
        self.active = True
        self.cost = None
        self.premium = None
        self.discount = 0
        self.feed = feed

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

    def net_fare(self) -> int:
        return self.cost - self.discount


class FixedFare(BaseFare):
    def __init__(self, start_time, max_transfers, max_time, feed):
        super().__init__(start_time, max_transfers, max_time, feed)

    def __repr__(self) -> str:
        active = ["X", "A"][int(self.active)]
        return f"<FixedFare {active} {self.start_time} | {self.transfers:06}t | {self.max_time:04}s | +{self.cost:04}¢ | -{self.discount:04}¢>"


class ZoneFare(BaseFare):
    def __init__(
        self, start_time, max_transfers, max_time, feed, route_id, from_zone, to_zone
    ):
        super().__init__(start_time, max_transfers, max_time, feed)
        self.route_id = route_id
        self.from_zone = from_zone
        self.to_zone = to_zone

    def __repr__(self) -> str:
        active = ["X", "A"][int(self.active)]
        return f"<ZoneFare  {active} {self.start_time} | {self.transfers:06}t | {self.max_time:04}s | +{self.cost:04}¢ | -{self.discount:04}¢ | {self.from_zone}->{self.to_zone} >"

    def update_fare(self):
        """Update the fare based on start and end zones"""
        sql = f"""
        SELECT zf.fare_cost
        FROM zone_fare zf 
        WHERE mdb_slug = '{self.feed}' 
        AND route_id = '{self.route_id}'
        AND from_zone = '{self.from_zone}'
        AND to_zone = '{self.to_zone}'"""
        res = execute_sql(sql)[0][0]
        self.cost = int(res)
