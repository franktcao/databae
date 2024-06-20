from pathlib import Path

import pandas as pd 

from eda_utils import get_config, get_project_root

def process_diagnosis(
    diag_table: pd.DataFrame,  columns: dict[str, dict]
) -> pd.DataFrame:
    processed = diag_table.copy()
    for out_col, spec in columns.items():
        col, contains = spec.items()
        processed = (
            processed
            .assign(
                **{
                    out_col: lambda x: (
                        x[col].str.contains(contains, case=False, regex=True)
                    )
                }
            )
        )

    processed = (
        processed 
        .rename(columns={"Patient Id": "Patient id"})
        .filter(["Patient id", *(columns.keys())])
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
    columns = config["columns"]
    processed_diag = process_diagnosis(diag_table, columns)
    processed_main = process_main_table(main_table, columns)
    result = (
        processed_main
        .merge(processed_diag, on="Patient id", how="left")
        .fillna({col: 0 for col in columns})
    )
    
    data_path = Path(config["datapath"])
    fname_main = table_names["main"]
    fpath = data_path / fname_main
    fpath = fpath.with_stem(f"{fpath.stem}_w_1hots")
    result.to_excel(fpath, index=False)
    
    print(f"Result preview:")
    print(result.head())
    print(f"Result saved to {fpath}")


if __name__ == "__main__":
    config = get_config("one_hots.yml")
    
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