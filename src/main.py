from functools import reduce
from pathlib import Path

import numpy as np
import pandas as pd
import pandera as pa 
import yaml

from dataclass import FilterConfig


def get_config():
    fname = "tables.yml"
    fpath = Path(__file__).with_name(fname)
    with open(fpath) as stream:
        result = yaml.safe_load(stream)
    
    return result


def get_manifest(tables_by_type: dict[str, list[str]]) -> pd.DataFrame:
    result = (
        pd.DataFrame(
            data=[
                (type_, name) 
                for type_, tables in tables_by_type.items() 
                for name in tables
            ],
            columns=["table_type", "table_name"],
        )
        .set_index("table_name")
    )

    return result


def main(config: dict[str, str | float]) -> None:
    # Generate manifest/driver from config
    tables_by_type = config["tables"]
    manifest = get_manifest(tables_by_type)
    
    # Get data
    datapath = Path(config["datapath"])
    tables = {
        table_name: pd.read_csv((datapath / table_name).with_suffix(".csv")) 
        for table_name, _ in manifest.iterrows()
    }
    
    # Filter data
    tables_to_filter = config["filter"]
    for table_name, df in tables.items():
        # Check to see filtering is needed
        specs = tables_to_filter.get(table_name)
        if specs is None:
            continue
        
        method = specs["method"] 
        combine = specs["combine"]

        # Validate filter config, but don't need
        _ = FilterConfig(method=method, combine=combine)

        columns = specs["columns"]
        conditions = [df[col].isin(entries) for col, entries in columns.items()]
        if combine == "or":
            condition = reduce(lambda x, y: x | y, conditions)
        else:
            condition = reduce(lambda x, y: x & y, conditions)
        
        if method == "drop":
            condition = ~condition
        
        filtered = df[condition]
        
        tables[table_name] = filtered
        

    # === Collect multi-row keys
    groupby_cols = config["global_keys"]
    for table_name, df in tables.items():
        table_type = manifest.loc[table_name, "table_type"]

        if table_type == "single_row_per_key":
            continue
        
        grouped = df.groupby(groupby_cols, as_index=False).agg(list)

        tables[table_name] = grouped


    # === Join all columns together
    result = reduce(
        lambda x, y: x.merge(y, on=groupby_cols, how="left"), tables.values()
    )

    # === Validate that main key is unique
    schema = pa.DataFrameSchema(unique=groupby_cols)
    schema.validate(result)

    print(result)


if __name__ == "__main__":
    config = get_config()
    main(config)