import pandas as pd
import os

base_dir = "data"
years = [2015, 2016, 2017, 2018, 2019]
all_data = []

for year in years:
    casualty_file = os.path.join(
        base_dir, f"road-crash-data-{year}", f"{year}_DATA_SA_Casualty.csv"
    )
    crash_file = os.path.join(
        base_dir, f"road-crash-data-{year}", f"{year}_DATA_SA_Crash.csv"
    )
    units_file = os.path.join(
        base_dir, f"road-crash-data-{year}", f"{year}_DATA_SA_Units.csv"
    )

    casualty_df = pd.read_csv(casualty_file)
    crash_df = pd.read_csv(crash_file)
    units_df = pd.read_csv(units_file)

    units_df.rename(
        columns={
            "Sex": "Unit Sex",
            "Age": "Unit Age",
            "Postcode": "Unit Postcode",
            "Unit No": "UND_UNIT_NUMBER",
        },
        inplace=True,
    )

    merged_df = pd.merge(crash_df, units_df, on="REPORT_ID", how="left")
    merged_df = pd.merge(
        merged_df,
        casualty_df,
        on=["REPORT_ID", "UND_UNIT_NUMBER"],
        how="left",
    )

    merged_df = merged_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    merged_df["Veh Reg State"].fillna("UNKNOWN", inplace=True)
    merged_df["Lic State"].fillna("UNKNOWN", inplace=True)
    merged_df["Veh Year"].fillna(9999, inplace=True)
    merged_df["Veh Year"].replace(["", "XXXX"], 9999, inplace=True)
    merged_df["Unit Age"].fillna(-1, inplace=True)
    merged_df["Unit Age"].replace(["", "XXX"], -1, inplace=True)
    merged_df["AGE"].replace(["", "XX", "XXX"], -1, inplace=True)
    merged_df["Licence Class"].fillna("Unknown", inplace=True)
    merged_df["Licence Type"].fillna("Unknown", inplace=True)
    merged_df["Towing"].fillna("Unknown", inplace=True)
    merged_df["Unit Postcode"].fillna(9999, inplace=True)
    merged_df["Unit Postcode"].replace(["XXX", "XXXX"], 9999, inplace=True)
    merged_df["Number Occupants"].fillna(-1, inplace=True)
    merged_df["Number Occupants"].replace(["XXX", "XXXX"], -1, inplace=True)
    merged_df["Licence Class"].replace(["XX"], "Unknown", inplace=True)

    all_data.append(merged_df)

final_df = pd.concat(all_data, ignore_index=True)
final_df.to_csv("combined_road_crash_data_2015_2019.csv", index=False)
