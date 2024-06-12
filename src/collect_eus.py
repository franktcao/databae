from pathlib import Path

import pandas as pd 

from eda_utils import get_config, get_project_root

def process_procedures(
    proc_table: pd.DataFrame, config: dict
) -> pd.DataFrame:
    keep_eus_ref = config["keep_eus_ref"]
    codes = config["codes_of_interest"]
    processed = (
        proc_table
        .astype({"Date": "datetime64[D]",})
        .sort_values(["Patient Id", "Date"])
        .assign(Date=lambda x: x["Date"].dt.date)
        .astype({"Date": "str"})
        .pipe(lambda x: x[x["Code"].isin(codes)])
        .groupby("Patient Id", as_index=False)
        .agg(list)
        .assign(
            **{
                "EUS dates": (
                    lambda x: (
                        x["Date"]
                        .apply(lambda y: dict.fromkeys(y).keys())
                        .apply(lambda y: "; ".join(y))
                    )
                ),
                "EUS": 1,
            }
        )
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
        .drop(columns=["Code", "Date"])
        .rename(columns={"Patient Id": "Patient id"})
    )

    return processed 


def main(
    main_table: pd.DataFrame, proc_table: pd.DataFrame, config: dict,
) -> None:
    processed = process_procedures(proc_table, config)
    result = (
        main_table
        .drop(columns=["EUS", "EUS dates"])
        .merge(processed, on="Patient id", how="left")
        .fillna({"EUS": 0})
        .astype({"EUS": int})
    )
    # print(result)
    data_path = Path(config["datapath"])
    fname_main = table_names["main"]
    fpath = data_path / fname_main
    fpath = fpath.with_stem(f"{fpath.stem}_w_eus")
    # print(fpath)
    result.to_csv(fpath, index=False)


if __name__ == "__main__":
    config = get_config("eus.yml")
    data_path = Path(config["datapath"])

    table_names = config["tables"]

    encoding = "utf-8"
    
    fname_main = table_names["main"]
    fpath = data_path / fname_main
    main_table = pd.read_csv(fpath, encoding=encoding)
    print(main_table)
    
    encoding = "latin-1"
    encoding = "ISO-8859-1"
    fname_proc = table_names["procedures"]
    fpath = data_path / fname_proc
    proc_table = pd.read_csv(fpath, encoding=encoding)
    print(proc_table)

    main(main_table, proc_table, config)