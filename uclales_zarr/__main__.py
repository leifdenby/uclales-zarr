from pathlib import Path

from .uclales_zarr import main

if __name__ == "__main__":
    import argparse

    argparser = argparse.ArgumentParser()
    argparser.add_argument("source_data_path", type=Path)
    argparser.add_argument("experiment_name")
    args = argparser.parse_args()

    import ipdb

    with ipdb.launch_ipdb_on_exception():
        main(fp_source_data=args.source_data_path, exp_name=args.experiment_name)
