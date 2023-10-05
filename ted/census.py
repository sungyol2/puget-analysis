import geopandas
import pandas
from pygris import block_groups


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
        xwk = pandas.read_csv(url, dtype={"tabblk2020": str, "bgrp": str})[["tabblk2020", "bgrp"]]
        both = pandas.merge(df, xwk, left_on="w_geocode", right_on="tabblk2020")
        both = both[["bgrp", "C000"]].groupby("bgrp", as_index=False).sum()
        dfs.append(both)
    all = pandas.concat(dfs, axis="index")
    print(
        "There are",
        block_groups[~block_groups[bg_column].isin(all.bgrp)].shape[0],
        "empty block groups, filling with 0",
    )
    merged = pandas.merge(block_groups, all, left_on="BG20", right_on="bgrp", how="left").fillna(0)[["BG20", "C000"]]
    merged["C000"] = merged["C000"].astype(int)
    return merged


def link_block_group_shapes(block_groups_2020: geopandas.GeoDataFrame, census_year: int):
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
