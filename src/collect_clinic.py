from pathlib import Path

import pandas as pd 

from eda_utils import get_config, get_project_root

def process_procedures(
    proc_table: pd.DataFrame, config: dict
) -> pd.DataFrame:
    keep_eus_ref = config["keep_ref"]
    codes = config["codes_of_interest"]
    providers = config["providers_of_interest"]
    providers_str = "|".join([x.upper() for x in providers])
    
    processed = (
        proc_table
        .astype({"Date": "datetime64[s]",})
        .sort_values(["Patient Id", "Date"])
        .assign(Date=lambda x: x["Date"].dt.date)
        .astype({"Date": "str"})
        .assign(
            in_perf=lambda x: (
                x["Performing Provider"]
                .str.contains(providers_str, regex=True, case=False)
            ),
            in_bill=lambda x:( 
                x["Billing Provider"]
                .str.contains(providers_str, regex=True, case=False)
            ),
            seen_by_team=lambda x: x["in_perf"] | x["in_bill"]
        )
        .pipe(lambda x: x[x["Code"].isin(codes)])
        .pipe(lambda x: x[x["seen_by_team"]])
        .groupby("Patient Id", as_index=False)
        .agg(list)
        .assign(
            **{
                "Clinic dates": (
                    lambda x: (
                        x["Date"]
                        .apply(lambda y: dict.fromkeys(y).keys())
                        .apply(lambda y: "; ".join(y))
                    )
                ),
                "has_clinic_code": True,
            }
        )
        .assign(seen_in_clinic=1)

    )
    if keep_eus_ref == 1:
        processed =(
            processed
            .assign(
                eus_ref=lambda x: x.apply(
                    lambda row: [
                        (date, code) 
                        for date, code in zip(row["Date"], row["Code"])
                    ],
                    axis="columns"
                )
            )
        )

    processed = (
        processed 
        # .drop(columns=["Code", "Date"])
        .rename(columns={"Patient Id": "Patient id"})
        .filter(["Patient id", "Clinic dates", "seen_in_clinic"])
    )

    return processed 

def process_main_table(
    main_table: pd.DataFrame, config: dict
) -> pd.DataFrame:
    processed = (
        main_table
        .rename(columns={"ï»¿Patient id": "Patient id"})
        .drop(columns=["Clinic dates", "Seen in clinic"])
    )

    return processed


def main(
    main_table: pd.DataFrame, proc_table: pd.DataFrame, config: dict,
) -> None:
    processed_proc = process_procedures(proc_table, config)
    processed_main = process_main_table(main_table, config)
    result = (
        processed_main
        .merge(processed_proc, on="Patient id", how="left")
        .fillna({"seen_in_clinic": 0})
        .rename(columns={"seen_in_clinic": "Seen in clinic"})
    )
    
    data_path = Path(config["datapath"])
    fname_main = table_names["main"]
    fpath = data_path / fname_main
    fpath = fpath.with_stem(f"{fpath.stem}_w_clinic")
    result.to_excel(fpath, index=False)
    
    print(f"Result preview:")
    print(result.head())
    print(f"Result saved to {fpath}")


if __name__ == "__main__":
    config = get_config("clinic.yml")
    
    data_path = Path(config["datapath"])
    table_names = config["tables"]
    
    fname_main = table_names["main"]
    fpath = data_path / fname_main
    main_table = pd.read_excel(fpath)
    
    fname_proc = table_names["procedures"]
    fpath = data_path / fname_proc
    
    encoding = "utf-8"
    proc_table = pd.read_csv(fpath, encoding=encoding)

    main(main_table, proc_table, config)