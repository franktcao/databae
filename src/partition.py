from pathlib import Path

import pandas as pd
from eda_utils import get_project_root


def main():
    # file_size_gb = 3e9
    # desired_size_gb = 0.5e9
    rows_per_batch = 1e6
    data_path_in = get_project_root() / "data" / "to_partition"
    data_path_out = get_project_root() / "data" / "partitioned"
    data_path_out.mkdir(exist_ok=True)
    for fpath in data_path_in.glob("*.csv"):
        fname = Path(fpath).stem
        for batch, chunk in enumerate(
            pd.read_csv(fpath, chunksize=rows_per_batch, low_memory=False)
        ):
            fname_out = fpath.with_stem(f"{fname}_{batch}").name
            fpath_out = data_path_out / fname_out

            chunk.to_csv(fpath_out)


if __name__ == "__main__":
    main()