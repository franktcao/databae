from pathlib import Path

import pandas as pd

from eda_utils import get_project_root


def generate_sample_data() -> None:
    data_path = get_project_root() / "data" / "hri_in_bpc"
    df = pd.DataFrame(
        {
            "col_1": list("abc"),
            "col_2": ["apple", "banana", "calamansi"],
            "col_3": ["red", "yellow", "orange"],
            "col_4": ["sweet", "sweet", "sour"],
        }
    )
    fname = "table_1.csv"
    fpath = data_path / fname
    df.to_csv(fpath, index=False)

    df = pd.DataFrame(
        {
            "col_1": list("aaabb"),
            "col_5": ["big", "medium", "small", "small", "medium"],
            "col_6": [9.2, 6.3, 2.7, 5.8, 8.6],
        }
    )
    fname = "table_2.csv"
    fpath = data_path / fname
    df.to_csv(fpath, index=False)


if __name__ == "__main__":
    generate_sample_data()