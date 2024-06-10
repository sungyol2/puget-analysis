import datetime
import itertools
import json
import logging
import multiprocessing
import os
import requests
from pytz import timezone
import time

import geopandas
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
WALK_MODE = "WALK"

TRANSFER_DISCOUNT = "transfer-discount"


def compute_wmata_2020_fare(miles):
    """Based on formula provided by WMATA

    Parameters
    ----------
    miles : float
        Composite miles travelled

    Returns
    -------
    int
        Fare amount
    """
    if miles < 3:
        fare = 225
    elif miles <= 6:
        fare = 225 + (miles - 3) * 32.6
    else:
        fare = 225 + 3 * 32.6 + (miles - 6) * 28.8
    return min(int(round(fare)), 600)


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
        centroids = geopandas.read_file(self.gpkg, layer=self.centroids_layer)
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


def add_gtfs_tag_to_zipfiles(folder):
    for f in os.listdir(folder):
        if f.endswith(".zip"):
            os.rename(os.path.join(folder, f), os.path.join(folder, f"gtfs-{f}"))


def get_unique_routes_used(itinerary_parquet_file, output_file):
    df = pandas.read_parquet(itinerary_parquet_file)[["feed", "route_id"]]
    df = df[~df.feed.isna()]
    df = df.drop_duplicates()
    df.to_csv(output_file, index=False)


def run_r5_on_clusters(
    clusters: geopandas.GeoDataFrame,
    gtfs_folder: str,
    osm_file: str,
    departure: datetime,
    output_file,
):
    print("-> Running R5py on clusters <-")
    print("  GTFS Folder:", gtfs_folder)
    print("  OSM File:", osm_file)
    print("  Departure:", departure)
    print()
    clusters.rename(columns={"CLUSTER_ID": "id"}, inplace=True)
    # Read in the GTFS set
    gtfs_files = []
    for filename in os.listdir(gtfs_folder):
        gtfs_files.append(os.path.join(gtfs_folder, filename))

    # Build the full network
    print("  Building network")
    network = r5py.TransportNetwork(osm_pbf=osm_file, gtfs=gtfs_files)
    computer = r5py.TravelTimeMatrixComputer(
        network,
        origins=clusters,
        destinations=clusters,
        departure=departure,
        departure_time_window=datetime.timedelta(minutes=120),
        max_time=datetime.timedelta(minutes=180),
        transport_modes=["WALK", "TRANSIT"],
    )
    print("  Computing Travel Times")
    # Actually compute the travel times
    mx = computer.compute_travel_times()
    mx = mx.dropna(subset="travel_time")
    # Dump it into a folder
    mx[["from_id", "to_id"]].to_csv(output_file, index=False)


class Itinerary:
    def __init__(
        self, itinerary_df: pandas.DataFrame, region: str, db, verbose: bool = False
    ):
        self._df = itinerary_df.sort_values("segment")
        self.region = region
        self._df.departure_time = pandas.to_datetime(self._df.departure_time)
        self._legs = []
        self._fares = []
        self.verbose = verbose
        self.db = db

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
                this_leg = TransitLeg.from_row(row, prev_leg, self.db)
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

            # Let's go ahead and update all fare clocks
            self.update_fare_times(current_time)

            # Let's find out if the next leg is already covered by the existing fares
            df = from_leg.transfers[from_leg.transfers.to_mdb_slug == to_leg.feed]

            # Now we need to filter out rules specifically
            # First we check for an __ANY__ condition as that covers all routes
            from_df = df[df.from_route_id == "__ANY__"]
            if from_df.shape[0] == 0:
                # Next, we check for a specific route ID
                from_df = df[df.from_route_id == from_leg.route_id]

                if from_df.shape[0] == 0:
                    # Finally, we check for an __ELSE__ key
                    from_df = df[df.from_route_id == "__ELSE__"]

            # Now we check for the route we are transferring to
            to_df = from_df[from_df.to_route_id == "__ANY__"]
            if to_df.shape[0] == 0:
                # Next, we check for a specific route ID
                to_df = from_df[from_df.from_route_id == from_leg.route_id]
                if to_df.shape[0] == 0:
                    # Finally, we check for an __ELSE__ key
                    to_df = from_df[from_df.from_route_id == "__ELSE__"]

            if to_df.shape[0] > 0:
                # We have some kind of rule, let's apply the first one we find (should just be one)
                tfr = to_df.iloc[0]
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
                if self.verbose == True:
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
        res = execute_sql(sql, self.db)[0]

        if res[0] == "flat":
            fare = FixedFare(leg.departure_time, res[1], res[2], leg.feed, self.db)
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
                db=self.db,
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
        try:
            start_zone = execute_sql(sql, self.db)[0][0]
        except IndexError:
            raise IndexError(
                f"{leg.feed} failed to find a zone for stop {leg.start_stop_id}"
            )

        sql = f"""
        SELECT zone_id
        FROM "zone"
        where "zone".mdb_slug = '{leg.feed}'
        and "zone".stop_id = '{leg.end_stop_id}'
        """
        end_zone = execute_sql(sql, self.db)[0][0]

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
        res = execute_sql(sql, self.db)
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

        return int(execute_sql(sql, self.db)[0][0])


def execute_sql(sql, db) -> list:
    conn = sqlite3.connect(db)
    cursor = conn.cursor()
    cursor.execute(sql)
    res = cursor.fetchall()
    cursor.close()
    return res


def execute_sql_to_df(sql, db) -> pandas.DataFrame:
    conn = sqlite3.connect(db)
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
        db,
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
        self.db = db

        # Let's get the transfers
        sql = f"""
        SELECT * from transfer
        where transfer.from_mdb_slug = '{self.feed}'
        """
        self.transfers = execute_sql_to_df(sql, self.db)

    def __repr__(self) -> str:
        return f"<TransitLeg {self.transport_mode} {self.departure_time} | {self.route_id}:{self.start_stop_id}->{self.end_stop_id}>"

    @classmethod
    def from_row(cls, r, prev_leg, db):
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
            db,
        )
        # Link the list
        if prev_leg is not None:
            leg.prev_leg.next_leg = leg

        return leg


class BaseFare:
    def __init__(self, start_time, transfers, duration, feed, db):
        self.start_time = start_time
        self.active = True
        self.cost = None
        self.premium = None
        self.discount = 0
        self.feed = feed
        self.db = db

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
    def __init__(self, start_time, max_transfers, max_time, feed, db):
        super().__init__(start_time, max_transfers, max_time, feed, db)

    def __repr__(self) -> str:
        active = ["X", "A"][int(self.active)]
        return f"<FixedFare {active} {self.start_time} | {self.transfers:06}t | {self.max_time:04}s | +{self.cost:04}¢ | -{self.discount:04}¢>"


class ZoneFare(BaseFare):
    def __init__(
        self,
        start_time,
        max_transfers,
        max_time,
        feed,
        db,
        route_id,
        from_zone,
        to_zone,
    ):
        super().__init__(start_time, max_transfers, max_time, feed, db)
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
        AND (route_id = '{self.route_id}' OR route_id = '__ANY__')
        AND from_zone = '{self.from_zone}'
        AND to_zone = '{self.to_zone}'"""
        res = execute_sql(sql, self.db)
        if len(res) == 0:
            # Try the reverse
            sql = f"""
                    SELECT zf.fare_cost
                    FROM zone_fare zf 
                    WHERE mdb_slug = '{self.feed}' 
                    AND (route_id = '{self.route_id}' OR route_id = '__ANY__')
                    AND from_zone = '{self.to_zone}'
                    AND to_zone = '{self.from_zone}'"""
            res = execute_sql(sql, self.db)
        if len(res) > 0:
            self.cost = int(res[0][0])
        else:
            self.cost = 500


class OTPQuery:
    OTP_ENDPOINT = "http://localhost:8080/otp/routers/default/index/graphql"

    def __init__(self, feeds):
        self.feeds = feeds

    def query_route(
        self,
        from_id: str,
        to_id: str,
        from_lat: float,
        from_lon: float,
        to_lat: float,
        to_lon: float,
        start_datetime: datetime.datetime,
    ) -> pandas.DataFrame:
        q = f"""
            {{
                plan(
                    from: {{ lat: {from_lat}, lon: {from_lon} }},
                    to: {{ lat: {to_lat}, lon: {to_lon} }},
                    date: "{start_datetime.strftime("%Y-%m-%d")}",
                    time: "{start_datetime.strftime("%H:%M")}",
                    transportModes: [
                        {{
                            mode: WALK
                        }},
                        {{
                            mode: TRANSIT
                        }},
                    ]) {{
                    itineraries {{
                        startTime
                        endTime
                        legs {{
                            mode
                            startTime
                            endTime
                            from {{
                                stop{{
                                gtfsId
                                }}
                                departureTime
                            }}
                            to {{
                                stop{{
                                gtfsId
                                }}
                                departureTime
                            }}
                            route {{
                                gtfsId
                                agency{{
                                    gtfsId
                                    name
                                }}
                            }}
                        }}
                    }}
                }}
            }}
            """
        r = requests.post(self.OTP_ENDPOINT, json={"query": q})
        itineraries = json.loads(r.text)["data"]["plan"]["itineraries"]
        leg_data = {
            "from_id": [],
            "to_id": [],
            "option": [],
            "segment": [],
            "mode": [],
            "departure_time": [],
            "feed": [],
            "agency_id": [],
            "route_id": [],
            "start_stop_id": [],
            "end_stop_id": [],
        }
        option_data = {"option": [], "startTime": [], "endTime": []}
        for idx, itinerary in enumerate(itineraries):
            option_data["option"].append(idx)
            option_data["startTime"].append(itinerary["startTime"])
            option_data["endTime"].append(itinerary["endTime"])
            for lidx, leg in enumerate(itinerary["legs"]):
                leg_data["from_id"].append(from_id)
                leg_data["to_id"].append(to_id)
                leg_data["option"].append(idx)
                leg_data["segment"].append(lidx)
                leg_data["mode"].append(leg["mode"])
                departure_time = datetime.datetime.fromtimestamp(
                    leg["from"]["departureTime"] / 1000, timezone("America/New_York")
                )
                leg_data["departure_time"].append(departure_time)
                if leg["from"]["stop"] != None:
                    leg_data["start_stop_id"].append(
                        leg["from"]["stop"]["gtfsId"].split(":")[1]
                    )
                else:
                    leg_data["start_stop_id"].append(None)
                if leg["to"]["stop"] != None:
                    leg_data["end_stop_id"].append(
                        leg["to"]["stop"]["gtfsId"].split(":")[1]
                    )
                else:
                    leg_data["end_stop_id"].append(None)
                if leg["route"] != None:
                    leg_data["route_id"].append(leg["route"]["gtfsId"].split(":")[1])
                    leg_data["agency_id"].append(
                        leg["route"]["agency"]["gtfsId"].split(":")[1]
                    )
                    leg_data["feed"].append(
                        self.feeds[str(leg["route"]["agency"]["gtfsId"].split(":")[0])]
                    )
                else:
                    leg_data["route_id"].append(None)
                    leg_data["agency_id"].append(None)
                    leg_data["feed"].append(None)
        leg_df = pandas.DataFrame(leg_data)
        options_df = pandas.DataFrame(option_data)
        options_df["delta"] = (options_df.endTime - options_df.startTime) / 60000
        try:
            option = options_df.sort_values("delta").iloc[0].option.astype(int)
            return leg_df[leg_df["option"] == option].drop(columns=["option"])
        except IndexError:
            return leg_df


def _route_query(param_list):
    otp = param_list[0]
    return otp.query_route(
        from_id=param_list[1],
        to_id=param_list[2],
        from_lat=param_list[3],
        from_lon=param_list[4],
        to_lat=param_list[5],
        to_lon=param_list[6],
        start_datetime=param_list[7],
    )


def map_fare_matrix_to_bg(
    fare_matrix_filepath: str,
    cluster_to_bg: str,
    gpkg: str,
    output_parquet: str,
    infinite_fare=9999,
):
    print("Mapping fare matrix to block groups")
    all_bgs = geopandas.read_file(gpkg, layer="bg_centroids")
    all_bgs = all_bgs["BG20"].tolist()
    all_bgs = list(itertools.product(all_bgs, all_bgs))
    all_bgs = pandas.DataFrame(
        all_bgs,
        columns=["BG20_from", "BG20_to"],
    )

    if fare_matrix_filepath.endswith(".csv"):
        fmx = pandas.read_csv(fare_matrix_filepath)
    else:
        fmx = pandas.read_parquet(fare_matrix_filepath)
    c2bg = pandas.read_csv(cluster_to_bg, dtype={"BG20": str})
    df = pandas.merge(fmx, c2bg, left_on="from_id", right_on="CLUSTER_ID", how="left")
    df = pandas.merge(
        df,
        c2bg,
        left_on="to_id",
        right_on="CLUSTER_ID",
        how="left",
        suffixes=["_from", "_to"],
    )
    df = df[["BG20_from", "BG20_to", "fare_cost"]]
    df = pandas.merge(all_bgs, df, on=["BG20_from", "BG20_to"], how="left")
    df["fare_cost"] = df["fare_cost"].fillna(infinite_fare)

    df.to_parquet(output_parquet)


def make_fare_matrix_from_itineraries(
    itineraries_parquet, matrix_parquet, fares_db, region_key
):
    df = pandas.read_parquet(itineraries_parquet).rename(
        columns={"mode": "transport_mode"}
    )
    pairs = df.drop_duplicates(subset=["from_id", "to_id"])
    fares = {"from_id": [], "to_id": [], "fare_cost": []}
    for idx, pair in tqdm(pairs.iterrows(), total=pairs.shape[0]):
        sub_df = df[
            (df.from_id == pair["from_id"]) & (df.to_id == pair["to_id"])
        ].copy()
        if sub_df.shape[0] > 1:
            it = Itinerary(
                sub_df,
                region_key,
                fares_db,
            )
            it.clean()
            it.make_legs()
            fares["from_id"].append(pair["from_id"])
            fares["to_id"].append(pair["to_id"])
            fares["fare_cost"].append(it.compute_fare())

        fare_df = pandas.DataFrame(fares)
        fare_df.to_parquet(matrix_parquet)


def _chunkify(l: list, n: int):
    """Divide a list into chunks of size n
    Parameters
    ----------
    l : list
        The list to chunkify
    n : int
        The maximum chunk size
    """
    for i in range(0, len(l), n):
        yield l[i : i + n]


def dechunkify(chunk_folder: str, parquet_output: str):
    print("Dechunkifying")
    dfs = []
    for f in os.listdir(chunk_folder):
        dfs.append(
            pandas.read_csv(
                os.path.join(chunk_folder, f),
                dtype={
                    "from_id": int,
                    "to_id": int,
                    "route_id": str,
                    "stop_id": str,
                    "feed": str,
                    "start_stop_id": str,
                    "end_stop_id": str,
                },
            )
        )

    df = pandas.concat(dfs, axis="index")
    df["agency_id"] = df["agency_id"].astype(str)
    df.to_parquet(parquet_output)


def run_otp_itineraries_from_pairs_list(
    fares_yaml: str,
    pairs_df: pandas.DataFrame,
    clusters: pandas.DataFrame,
    departure: datetime.datetime,
    output_folder: str,
    region_key: str,
    chunk_size=30,
):
    """Fetch a set of OTP itineraries based on a provided set of origin-destination pairs

    Parameters
    ----------
    fares_yaml : str
        The file path of the fares configuration YAML file
    pairs_df : pandas.DataFrame
        The dataframe with the pairs to check
    clusters : pandas.DataFrame
        The set of clusters to create itineraries for
    departure : datetime.datetime
        The departure time and date to use
    output_folder : str
        The folder in which to put the chunks that are created
    region_key : str
        The region key string (e.g. WAS)
    chunk_size : int, optional
        The size of chunk to use (number of pairs), by default 30
    """
    print("Running OTP Itineraries - Specified Pairs")
    with open(fares_yaml) as infile:
        config = yaml.safe_load(infile)

    feeds = {
        (str(key) if isinstance(key, int) else key): config["feeds"][key]
        for key in config["feeds"]
    }

    otp = OTPQuery(feeds)
    dfs = []
    params_list = []
    print("  Building job list")

    # Remove diagnonals
    pairs_df = pairs_df[pairs_df.from_id != pairs_df.to_id]
    pairs_df = pandas.merge(
        pairs_df,
        clusters,
        left_on="from_id",
        right_on="CLUSTER_ID",
        how="left",
    )
    pairs_df = pandas.merge(
        pairs_df,
        clusters,
        left_on="to_id",
        right_on="CLUSTER_ID",
        how="left",
        suffixes=["_o", "_d"],
    )
    for idx, pair in pairs_df.iterrows():
        params_list.append(
            [
                otp,
                pair.CLUSTER_ID_o,
                pair.CLUSTER_ID_d,
                pair.MEAN_Y_o,
                pair.MEAN_X_o,
                pair.MEAN_Y_d,
                pair.MEAN_X_d,
                departure,
            ]
        )
    print("  Generating", len(params_list), "itineraries")
    chunk_list = list(_chunkify(params_list, chunk_size))
    print(f"  Using chunks of size {chunk_size}")
    cpus = multiprocessing.cpu_count() - 2
    print(f"  Using {cpus} CPUs")

    start = time.time()
    with multiprocessing.Pool(cpus) as p:
        for idx, chunk in tqdm(enumerate(chunk_list), total=len(chunk_list)):
            df_list = p.map(_route_query, chunk)
            df_list = [d for d in df_list if not d.empty]
            if len(df_list) > 0:
                pandas.concat(df_list, axis="index").to_csv(
                    f"{output_folder}/{region_key}_{idx}.csv", index=False
                )
            else:
                print("    Chunk", idx, "has no data")

    end = time.time()
    print("  Took", end - start, "seconds")


def run_otp_itineraries_in_parallel(fares_yaml, points, output_folder, chunk_size=30):
    with open(fares_yaml) as infile:
        config = yaml.safe_load(infile)

    feeds = {
        (str(key) if isinstance(key, int) else key): config["feeds"][key]
        for key in config["feeds"]
    }

    otp = OTPQuery(feeds)
    dfs = []
    params_list = []
    for odx, origin in points.iterrows():
        for ddx, dest in points.iterrows():
            if origin.cluster_id != dest.cluster_id:
                params_list.append(
                    [
                        otp,
                        origin.cluster_id,
                        dest.cluster_id,
                        origin.MEAN_Y,
                        origin.MEAN_X,
                        dest.MEAN_Y,
                        dest.MEAN_X,
                        datetime.datetime(2023, 9, 27, 7, 11),
                    ]
                )
    print("Generating", len(params_list), "itineraries")
    chunk_list = list(_chunkify(params_list, chunk_size))
    print(f"Using chunks of size {chunk_size}")
    cpus = multiprocessing.cpu_count() - 2
    print(f"Using {cpus} CPUs")

    start = time.time()
    with multiprocessing.Pool(cpus) as p:
        for idx, chunk in tqdm(enumerate(chunk_list), total=len(chunk_list)):
            df_list = p.map(_route_query, chunk)
            df_list = [d for d in df_list if not d.empty]
            if len(df_list) > 0:
                pandas.concat(df_list, axis="index").to_csv(
                    f"{output_folder}/WAS_{idx}.csv", index=False
                )
            else:
                print("Chunk", idx, "has no data")

    end = time.time()
    print("Took", end - start, "seconds")
