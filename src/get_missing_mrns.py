
from pathlib import Path
import pandas as pd

from eda_utils import get_config


def main(
    main_table: pd.DataFrame, cgdb: pd.DataFrame, mrn_col: str,
) -> None:
    result = (
        main_table[[mrn_col]]
        .merge(cgdb[[mrn_col]], on=mrn_col, how="left", indicator=True)
        .pipe(lambda x: x[x["_merge"] != "both"])
        .filter([mrn_col])
    )
    
    data_path = Path(config["datapath"])
    fname_main = table_names["main"]
    fpath = data_path / fname_main
    fpath = fpath.with_stem(f"missing_mrns")
    result.to_excel(fpath, index=False)
    
    print(f"Result preview:")
    print(result.head())
    print(f"Result saved to {fpath}")


if __name__ == "__main__":
    config = get_config("missing_mrns.yml")
    
    data_path = Path(config["datapath"])
    table_names = config["tables"]
    mrn_col = config["mrn_col"]
    
    fname_main = table_names["main"]
    fpath = data_path / fname_main
    main_table = pd.read_excel(fpath)
    
    cgdb = table_names["caner_gen_db"]
    fpath = data_path / cgdb
    
    cgdb_table = pd.read_excel(fpath)
    print(cgdb_table)

    main(main_table, cgdb_table, mrn_col)