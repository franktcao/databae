from pathlib import Path
import pandas as pd
import pandera as pa 
import yaml

import eda_utils as eb


def main() -> None:
    # Filter data

    # Collect multi-row keys

    # Validate that main key is unique
    
    # Join all columns together

    # Validate that main key is unique
    pass


def get_config():
    fname = "tables.yml"
    fpath = Path(__file__).with_name(fname)
    with open(fpath) as stream:
        result = yaml.safe_load(stream)
    
    return result


if __name__ == "__main__":
    config = get_config()
    df = pd.DataFrame(
        {
            "col_1": list("abc"),
            "col_2": ["apple", "banana", "calamansi"],
            "col_3": ["red", "yellow", "orange"],
            "col_4": ["sweet", "sweet", "sour"],
        }
    )
    fname = "table_1.csv"
    df = pd.DataFrame(
        {
            "col_1": list("aaabb"),
            "col_5": ["big", "medium", "small", "small", "medium"],
            "col_6": [9.2, 6.3, 2.7, 5.8, 8.6],
        }
    )
    fname = "table_2.csv"
    fpath = Path(config["datapath"]) / fname

    # df.to_csv(fpath, index=False)

    # df = pd.read_csv(fpath)
    # print(df)
    # print(config)
    # main(config)