from pathlib import Path

from .uclales_zarr import main

if __name__ == "__main__":
    import argparse

    argparser = argparse.ArgumentParser()
    argparser.add_argument("source_data_path", type=Path)
    argparser.add_argument("experiment_name")
    argparser.add_argument("--data-kind", default="3d")
    args = argparser.parse_args()

    main(
        fp_source_data=args.source_data_path,
        exp_name=args.experiment_name,
        data_kind=args.data_kind,
    )
