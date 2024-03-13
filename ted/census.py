import geopandas
import pandas
from pygris import block_groups, tracts
from pygris.data import get_census

demographic_categories = {
    "B03002_001E": "Everyone",
    "B03002_003E": "White People",
    "B03002_004E": "Black People",
    "B03002_006E": "Asian",
    "B03002_012E": "Hispanic or Latino",
    "B11003_016E": "Single Mother",
}

total_hhld = "B11001_001E"
zero_car_hhld = "B08201_002E"

age_categories = [
    "B01001_020E",
    "B01001_021E",
    "B01001_022E",
    "B01001_023E",
    "B01001_024E",
    "B01001_025E",
    "B01001_044E",
    "B01001_045E",
    "B01001_046E",
    "B01001_047E",
    "B01001_048E",
    "B01001_049E",
]

poverty_categories = [
    "C17002_002E",
    "C17002_003E",
    "C17002_004E",
    "C17002_005E",
    "C17002_006E",
    "C17002_007E",
]


def get_state_tracts_by_year(states: list, year: int) -> geopandas.GeoDataFrame:
    gdfs = []
    for state in states:
        gdfs.append(
            tracts(str(state).lower(), year=year)[["GEOID", "geometry"]].rename(
                columns={"GEOID": f"TR{str(year)[2:]}"}
            )
        )
    return pandas.concat(gdfs, axis="index")


def get_state_tract_centroids(states: list) -> geopandas.GeoDataFrame:
    gdfs = []
    for state in states:
        df = pandas.read_csv(
            f"https://www2.census.gov/geo/docs/reference/cenpop2020/tract/CenPop2020_Mean_TR{state}.txt",
            dtype={"COUNTYFP": str, "STATEFP": str, "TRACTCE": str, "BLKGRPCE": str},
        )
        df["TR20"] = df.STATEFP + df.COUNTYFP + df.TRACTCE
        gdfs.append(
            geopandas.GeoDataFrame(
                df, geometry=geopandas.points_from_xy(df.LONGITUDE, df.LATITUDE)
            )
        )
    return pandas.concat(gdfs, axis="index")


def get_state_block_groups_by_year(states: list, year: int) -> geopandas.GeoDataFrame:
    """Fetch block group spatial data for a given year

    Parameters
    ----------
    states : list
        A list of state FIPS codes or 2-letter codes
    year : int
        The census year to fetch

    Returns
    -------
    geopandas.GeoDataFrame
        A GeoDataFrame containing the block group information
    """
    gdfs = []
    for state in states:
        gdfs.append(
            block_groups(str(state).lower(), year=year)[["GEOID", "geometry"]].rename(
                columns={"GEOID": f"BG{str(year)[2:]}"}
            )
        )
    return pandas.concat(gdfs, axis="index")


def get_jobs_by_year(
    block_groups: pandas.DataFrame, states: list[str], bg_column="BG20", year=2020
) -> pandas.DataFrame:
    """Get jobs for a list of states and join to supplied block groups

    Fetch the LODES8 jobs data and attach it to a specified block group file
    using the crosswalk file.

    Parameters
    ----------
    block_groups : geopandas.DataFrame
        The block group regional defintion (2020 census data) list
    states : list[str]
        A list of **string representations of states**, not numbers.
    bg_column : str, optional
        The column name of the block group in the supplied dataframe, by default "BG20"
    year : int, optional
        The year to pull from, by default 2020

    Returns
    -------
    pandas.DataFrame
        A dataframe containing block-group-level job counts for all employment
    """
    dfs = []
    states = [i.lower() for i in states]
    for state in states:
        print(state)
        # Get the 2020 Data
        url = f"https://lehd.ces.census.gov/data/lodes/LODES8/{state}/wac/{state}_wac_S000_JT00_2020.csv.gz"
        df = pandas.read_csv(url, dtype={"w_geocode": str})[["w_geocode", "C000"]]
        # Get the Crosswalk data
        url = f"https://lehd.ces.census.gov/data/lodes/LODES8/{state}/{state}_xwalk.csv.gz"
        xwk = pandas.read_csv(url, dtype={"tabblk2020": str, "bgrp": str})[
            ["tabblk2020", "bgrp"]
        ]
        both = pandas.merge(df, xwk, left_on="w_geocode", right_on="tabblk2020")
        both = both[["bgrp", "C000"]].groupby("bgrp", as_index=False).sum()
        dfs.append(both)
    all = pandas.concat(dfs, axis="index")
    print(
        "There are",
        block_groups[~block_groups[bg_column].isin(all.bgrp)].shape[0],
        "empty block groups, filling with 0",
    )
    merged = pandas.merge(
        block_groups, all, left_on="BG20", right_on="bgrp", how="left"
    ).fillna(0)[["BG20", "C000"]]
    merged["C000"] = merged["C000"].astype(int)
    return merged


def download_demographic_data(
    block_groups: pandas.DataFrame, output_filepath: str
) -> pandas.DataFrame:
    """Fetch demographic data based on provided study area

    Returns
    -------
    DataFrame
        A pandas dataframe containing the demographic data for the impact area.
    """
    # See: https://api.census.gov/data/2021/acs/acs5
    # Read in the analysis centroids
    # api_key = self.settings["api_key"]
    print("Downloading demographic data")
    block_groups["state"] = block_groups["BG20"].str[:2]
    block_groups["county"] = block_groups["BG20"].str[2:5]
    states_and_counties = (
        block_groups[["state", "county"]].drop_duplicates().sort_values("state")
    )
    all_data = []
    # First we fetch all the easy ones
    variables = [i for i in demographic_categories.keys()]
    for idx, area in states_and_counties.iterrows():
        print(f"  Fetching State: {area['state']} County: {area['county']}")
        data = get_census(
            dataset="acs/acs5",
            year="2021",
            variables=variables,
            params={
                "for": "block group:*",
                "in": f"state:{area['state']} county:{area['county']}",
            },
            return_geoid=True,
        )

        # Age data
        age_data = get_census(
            dataset="acs/acs5",
            year="2021",
            variables=age_categories,
            params={
                "for": "block group:*",
                "in": f"state:{area['state']} county:{area['county']}",
            },
            return_geoid=True,
        )

        # Make sure they're numbers or summing goes very badly
        age_data[age_categories] = age_data[age_categories].astype(int)
        age_data["age_65p"] = age_data[age_categories].sum(axis="columns")

        data = pandas.merge(data, age_data[["age_65p", "GEOID"]], on="GEOID")

        # Now we do low income data
        low_income_data = get_census(
            dataset="acs/acs5",
            year="2021",
            variables=poverty_categories,
            params={
                "for": "block group:*",
                "in": f"state:{area['state']} county:{area['county']}",
            },
            return_geoid=True,
        )
        low_income_data[poverty_categories] = low_income_data[
            poverty_categories
        ].astype(int)
        low_income_data["low_income"] = low_income_data[poverty_categories].sum(
            axis="columns"
        )

        data = pandas.merge(data, low_income_data[["low_income", "GEOID"]], on="GEOID")

        all_hhld = get_census(
            dataset="acs/acs5",
            year="2021",
            variables=[total_hhld],
            params={
                "for": "block group:*",
                "in": f"state:{area['state']} county:{area['county']}",
            },
            return_geoid=True,
        )
        all_hhld["tract_id"] = all_hhld["GEOID"].str[:-1]
        all_hhld[total_hhld] = all_hhld[total_hhld].astype(int)
        all_hhld_gb = (
            all_hhld[["tract_id", total_hhld]].groupby("tract_id", as_index=False).sum()
        )
        all_hhld = pandas.merge(all_hhld, all_hhld_gb, how="left", on="tract_id")
        # Create a proportion table for assignment
        all_hhld.columns = ["bg_hhld", "BG20", "tract_id", "tract_hhld"]
        all_hhld["proportion"] = all_hhld["bg_hhld"] / all_hhld["tract_hhld"]
        all_hhld = all_hhld[["BG20", "tract_id", "proportion"]]

        # Zero-car HHLD data
        zc_hhld = get_census(
            dataset="acs/acs5",
            year="2021",
            variables=[zero_car_hhld],
            params={
                "for": "tract:*",
                "in": f"state:{area['state']} county:{area['county']}",
            },
            return_geoid=True,
        )

        zc_hhld[zero_car_hhld] = zc_hhld[zero_car_hhld].astype(int)
        zc_all = pandas.merge(
            all_hhld, zc_hhld, how="left", left_on="tract_id", right_on="GEOID"
        )
        zc_all["zero_car_hhld"] = zc_all[zero_car_hhld] * zc_all["proportion"]
        zc_all["zero_car_hhld"] = zc_all["zero_car_hhld"].fillna(0).round().astype(int)

        # Rename columns back to match the pattern above
        zc_all = zc_all[["BG20", "zero_car_hhld"]]
        zc_all = zc_all.rename(columns={"BG20": "GEOID"})

        data = pandas.merge(data, zc_all[["GEOID", "zero_car_hhld"]], on="GEOID")

        all_data.append(data)

        # Make sure they're numbers or summing goes very badly
        age_data[age_categories] = age_data[age_categories].astype(int)
        age_data["age_65p"] = age_data[age_categories].sum(axis="columns")

    result = pandas.concat(all_data, axis="index")

    # Now let's do the age one

    # Rename our GEOID
    result = result.rename(columns={"GEOID": "BG20"})
    result = result[result["BG20"].isin(block_groups["BG20"])]
    # Move the BG20 column to the front
    bg_column = result.pop("BG20")
    result.insert(0, "BG20", bg_column)
    result.to_csv(output_filepath, index=False)
    print("  Finished downloading demographic data")
    return result


def link_block_group_shapes(
    block_groups_2020: geopandas.GeoDataFrame, census_year: int
):
    block_groups_2020["state"] = block_groups_2020["bg_id"].str[:2]
    # pull the block groups for that state
    dfs = []
    for state in block_groups_2020.state.unique():
        print(state)
        # Fetch that state
        pybg = block_groups(state=str(state), year=census_year, cb=False)
        pybg.to_crs(block_groups_2020.crs, inplace=True)
        pybg["area"] = pybg.geometry.area
        dfs.append(pybg)

    all_bg = pandas.concat(dfs, axis="index")
    joined = all_bg.intersects(block_groups_2020)
    # joined = gpd.sjoin(all_bg, block_groups_2020)[["bg_id", "GEOID", "area", "geometry"]]
    joined.to_file("test_join.gpkg")
    joined.rename(columns={"GEOID": f"bg_id_{census_year}"}, inplace=True)
    joined["frac_in_2020"] = joined["area"] / joined.geometry.area

    return joined
