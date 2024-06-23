from pathlib import Path

import numpy as np
import pandas as pd 

from eda_utils import get_config, get_project_root

def process_diagnosis(
    diag_table: pd.DataFrame, 
    desc_col: str, 
    columns_1: dict[str, dict], 
    columns_desc: dict[str, dict],
    columns_recheck: dict[str, dict],
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
                            None
                        )
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
                        None,
                    )
                }
            )
        )

    patient_col = "Patient Id"
    processed = (
        processed
        .groupby(patient_col, as_index=False)
        .agg(
            {col: "max" for col in columns_1} | 
            {col: lambda x: " || ".join(set(x)) for col in columns_desc} |
            {col: "max" for col in columns_recheck} | 
            {f"{col} notes": lambda x: " || ".join(set(x)) for col in columns_recheck} 
        )
        .rename(columns={patient_col: "Patient id"})
        .filter(["Patient id", *(columns_1.keys()), *(columns_desc.keys())])
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
    columns = columns_1 | columns_desc
    processed_diag = process_diagnosis(
        diag_table, "Description", columns_1, columns_desc, columns_recheck
    )
    processed_main = process_main_table(main_table, columns)
    result = (
        processed_main
        .merge(processed_diag, on="Patient id", how="left")
        .fillna({col: 0 for col in columns_1})
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