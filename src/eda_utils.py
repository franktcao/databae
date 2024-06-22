from functools import reduce
from pathlib import Path

import pandas as pd
import yaml


def get_project_root() -> Path:
    result = Path(__file__).parent.parent

    return result


def get_config(fname: str = "tables.yml"):
    fpath = Path(__file__).with_name(fname)
    with open(fpath) as stream:
        result = yaml.safe_load(stream)
    
    return result


def flatten_table(
    df: pd.DataFrame, index_cols: str | list[str]
) -> pd.DataFrame:
    if isinstance(index_cols, str):
        index_cols = [index_cols]

    collect_cols = [col for col in df.columns if col not in index_cols]

    result = (
        df
        .groupby(index_cols, as_index=False)
        .agg({col: list for col in collect_cols}) 
    )

    return result


def join_single_key_tables(key_col: str, **dfs: pd.DataFrame) -> pd.DataFrame:
    # Cast to `iter` to use the first item and iterate over the rest
    df_by_name = iter(dfs.items())
    
    # Get first name and table
    prev_name, result = next(df_by_name, None)

    # Loop over tables to reduce
    for name, df in df_by_name:
        result = result.merge(
            df, 
            on=key_col, 
            how="outer",
            # Added in case columns overlap and to keep columns unique
            suffixes=(f"__{prev_name}", f"__{name}")
        )

        prev_name = name

    return result


def filter_rows(
    df: pd.DataFrame, 
    col: str, 
    *, 
    to_keep: list[str] = None,
    to_drop: list[str] = None,
) -> pd.DataFrame:
    if col not in df.columns:
        raise ValueError(f"{col=} is not found in {df.columns=}")
    
    result = df.copy()

    if to_keep is not None:
        result = result.pipe(lambda x: x[x[col].isin(to_keep)])
    if to_drop is not None:
        result = result.pipe(lambda x: x[~x[col].isin(to_drop)])

    return result


def validate_unique_keys(df: pd.DataFrame, keys: list[str]) -> None:
    duplicates = get_duplicate_rows(df, cols=keys)
    if len(duplicates) != 0:
        raise ValueError(f"Contains duplicate keys:\n{duplicates}")
    
    return


def get_duplicate_rows(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    result = df.pipe(lambda x: x[x.duplicated(subset=cols, keep=False)])

    return result
