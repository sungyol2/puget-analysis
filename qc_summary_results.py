import os

import pandas as pd
import altair as alt

REGION = "PHL"
DATA_FOLDER = "/home/willem/Documents/Project/TED/data/"

demographic_categories = {
    "B03002_001E": "Everyone",
    "B03002_003E": "White",
    "B03002_004E": "Black ",
    "B03002_006E": "Asian",
    "B03002_012E": "Hispanic or Latino",
    "C17002_003E": "In Poverty",
    "B03002_012E": "Single Mother",
    "age_65p": "Age 65+",
    "zero_car_hhld": "Zero-Car Households",
    "low_income": "Low Income",
}
for measure in ["C000_c45", "grocery_t3", "auto_ratio_c45", "early_voting_t1"]:
    for tod in ["WEDAM", "WEDPM", "SATAM"]:

        df = pd.read_csv(
            os.path.join(
                DATA_FOLDER,
                "packaged",
                REGION,
                "summary",
                f"summary_{REGION}_{tod}.csv",
            )
        )
        df = df[df.demographic != "C17002_003E"]
        df["demo_name"] = df.demographic.map(demographic_categories)
        df["auto_ratio_c45"] = df["C000_c45"] / df["C000_c45_auto"]

        to_plot = df[["demo_name", measure, "date"]].melt(id_vars=["demo_name", "date"])

        chart = (
            alt.Chart(to_plot)
            .mark_line()
            .encode(
                alt.X("date:T", title="").axis(format="%Y %B"),
                alt.Y("value:Q", title="Access Measure (total, minutes, or ratio)"),
                alt.Color("demo_name", title="Demographic"),
            )
            .properties(
                title={
                    "text": f"{tod} Equity of Access in {REGION}",
                    "subtitle": measure,
                },
                width=900,
                height=600,
            )
            .configure(
                font="Atkinson Hyperlegible",
            )
            .configure_view(strokeWidth=0)
            .configure_axis(grid=False, labelFontSize=12, titleFontSize=14)
            .configure_title(fontSize=22, anchor="start")
        )

        chart.save(
            f"/home/willem/Documents/Project/TED/data/region/{REGION}/qc/summary/summary_{REGION}_{tod}_{measure}.png"
        )
        print("Chart Saved")
