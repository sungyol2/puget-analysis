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
