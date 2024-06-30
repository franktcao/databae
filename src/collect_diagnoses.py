from pathlib import Path

import numpy as np
import pandas as pd 

from eda_utils import get_config, get_project_root


def get_entries_contain(
    sr: pd.Series, contains: list[str], **kwargs
) -> pd.Series:
    contains_str = "|".join(contains)

    result = sr.str.contains(contains_str, case=False, regex=True, **kwargs)

    return result


def get_panc_cancer_rows(
    df: pd.DataFrame,
    desc_col: str,
    exact_cols: dict[str, list[str]], 
    includes_cols: dict[str, list[str]], 
    review_cols: list[str],
    out_col: str = "Pancreas cancer",
    date_col: str = "Date",
) -> pd.DataFrame:
    match_condition = pd.Series(0, index=df.index)
    for col, entries in (exact_cols | includes_cols).items():
        match_condition |= get_entries_contain(df[col], entries)

    review_condition = pd.Series(0, index=df.index)
    for col, entries in review_cols.item():
        review_condition |= get_entries_contain(df[col], entries)

    result = (
        df
        .assign(
            **{
                out_col: lambda x: match_condition.astype(int),
                f"{out_col} dx date": lambda x: np.where(
                    match_condition, x[date_col].astype("datetime64[s]"), pd.NaT
                ),
                f"{out_col} notes": lambda x: np.where(
                    review_condition, x[desc_col], ""
                ),
            }
        )
    )

    return result


def process_diagnosis(
    diag_table: pd.DataFrame, 
    desc_col: str, 
    columns_1: dict[str, dict], 
    columns_desc: dict[str, dict],
    columns_recheck: dict[str, dict],
    columns_1_desc_date: dict[str, dict],
) -> pd.DataFrame:
    processed = diag_table.copy()
    for out_col, contains in columns_1.items():
        contains_str = "|".join(contains)
        processed = (
            processed
            .assign(
                **{
                    out_col: lambda x: (
                        x[desc_col]
                        .str.contains(contains_str, case=False, regex=True)
                        .astype(int)
                    )
                }
            )
        )

    for out_col, contains in columns_recheck.items():
        contains_str = "|".join(contains)
        processed = (
            processed
            .assign(
                **{
                    out_col: lambda x: (
                        x[desc_col]
                        .str.contains(contains_str, case=False, regex=True)
                        .astype(int)
                    )
                }
            )
            .assign(
                **{
                    f"{out_col} notes": lambda x: np.where(
                        x[out_col] == 0,
                        np.where(
                            x[desc_col].str.contains(out_col, case=False),
                            x[desc_col],
                            ""
                        ),
                        ""
                    )
                }
            )
        )

    for out_col, contains in columns_desc.items():
        contains_str = "|".join(contains)
        processed = (
            processed
            .assign(
                **{
                    out_col: lambda x: np.where(
                        (
                            x[desc_col]
                            .str.contains(contains_str, case=False, regex=True)
                        ), 
                        x[desc_col], 
                        "",
                    )
                }
            )
        )

    for out_col, contains in columns_1_desc_date.items():
        contains_str = "|".join(contains)
        processed = (
            processed
            .assign(
                **{
                    out_col: lambda x: (
                        x[desc_col]
                        .str.contains(contains_str, regex=True)
                        .astype(int)
                    )
                }
            )
            .assign(
                **{
                    f"{out_col} notes": lambda x: np.where(
                        x[out_col] == 1, x[desc_col], ""
                    )
                }
            )
            .assign(
                **{
                    f"{out_col} dx date": lambda x: np.where(
                        x[out_col] == 1, 
                        x["Date"].astype("datetime64[s]"), 
                        pd.NaT
                    )
                }
            )
            .assign(
                **{
                    f"{out_col} dx date": (
                        lambda x: pd.to_datetime(x[f"{out_col} dx date"].dt.date)
                    )
                }
            )
        )
    
    exact_cols: dict[str, list[str]] = (
        config["panc_cancer_columns"]["Pancreas cancer"]["exact"]
    )
    includes_cols: dict[str, list[str]] = (
        config["panc_cancer_columns"]["Pancreas cancer"]["includes"]
    )
    review_cols: dict[str, list[str]] = (
        config["panc_cancer_columns"]["Pancreas cancer"]["review"]
    )

    processed = get_panc_cancer_rows(
        processed, desc_col, exact_cols, includes_cols, review_cols,
    )

    patient_col = "Patient Id"
    processed = (
        processed
        .groupby(patient_col, as_index=False)
        .agg(
            {
                col: "max" 
                for col in (
                    *columns_1,
                    *columns_recheck,
                    *columns_1_desc_date,
                    "Pancreas cancer"
                )
            } | 
            {
                col: lambda x: " || ".join(set([e for e in x if e != ""]))
                for col in columns_desc 
            } |
            {
                f"{col} notes": lambda x: (
                    " || ".join(set([e for e in x if e != ""])) 
                )
                for col in (
                    *columns_recheck, *columns_1_desc_date, "Pancreas cancer",
                )
            } |
            {
                f"{col} dx date": "min" for col in (
                    *columns_1_desc_date, "Pancreas cancer"
                )
            } 
        )
        .assign(
            **{
                f"{col} dx date": lambda x: x[f"{col} dx date"].dt.date
                for col in ("PCN", "Pancreas cancer")
            }
        )
        .rename(columns={patient_col: "Patient id"})
        .filter(
            [
                "Patient id", 
                *columns_1,
                *columns_desc,
                *columns_recheck,
                *columns_1_desc_date,
                "Pancreas cancer",
                *[
                    f"{col} notes" 
                    for col in [
                        *columns_1_desc_date,
                        *columns_recheck,
                        "Pancreas cancer"
                    ]
                ],
                *[
                    f"{col} dx date" 
                    for col in [*columns_1_desc_date, "Pancreas cancer"]
                ],
            ]
        )
    )

    return processed 


def process_main_table(
    main_table: pd.DataFrame, columns: dict[str, dict]
) -> pd.DataFrame:
    processed = (
        main_table
        .rename(columns={"ï»¿Patient id": "Patient id"})
        .drop(columns=list(columns))
    )

    return processed


def main(
    main_table: pd.DataFrame, diag_table: pd.DataFrame, config: dict,
) -> None:
    columns_1 = config["1_columns"]
    columns_desc = config["desc_columns"]
    columns_recheck = config["recheck_columns"]
    columns_1_desc_date = config["1_desc_date_columns"]

    processed_diag = process_diagnosis(
        diag_table, 
        "Description", 
        columns_1, 
        columns_desc, 
        columns_recheck,
        columns_1_desc_date
    )

    # Drop all columns from main table that we will be filling 
    columns = (
        *columns_1, 
        *columns_desc,
        *columns_recheck,
        *columns_1_desc_date,
        "Pancreas cancer",
        *[
            f"{col} notes"
            for col in (
                *columns_recheck, *columns_1_desc_date, "Pancreas cancer"
            )
        ], 
        *[
            f"{col} dx date"
            for col in (*columns_1_desc_date, "Pancreas cancer")
        ]
    )
    processed_main = process_main_table(main_table, columns)

    result = (
        processed_main
        .merge(processed_diag, on="Patient id", how="left")
        .fillna({col: 0 for col in [*columns_1, "Pancreas cancer"]})
    )
    
    data_path = Path(config["datapath"])
    fname_main = table_names["main"]
    fpath = data_path / fname_main
    fpath = fpath.with_stem(f"{fpath.stem}_w_diag")
    result.to_excel(fpath, index=False)
    
    print(f"Result preview:")
    print(result.head())
    print(f"Result saved to {fpath}")


if __name__ == "__main__":
    config = get_config("diagnoses.yml")
    
    data_path = Path(config["datapath"])
    table_names = config["tables"]
    
    fname_main = table_names["main"]
    fpath = data_path / fname_main
    main_table = pd.read_excel(fpath)
    
    fname_proc = table_names["diagnosis"]
    fpath = data_path / fname_proc
    
    encoding = "utf-8"
    diag_table = pd.read_csv(fpath, encoding=encoding)

    main(main_table, diag_table, config)