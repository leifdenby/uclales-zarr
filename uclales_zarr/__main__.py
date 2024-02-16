from pathlib import Path

import dask.distributed
from loguru import logger

from .rechunking import rechunk_and_write
from .uclales_zarr import main

if __name__ == "__main__":
    import argparse

    argparser = argparse.ArgumentParser()
    argparser.add_argument("source_data_path", type=Path)
    argparser.add_argument("experiment_name")
    argparser.add_argument("--data-kind", default="3d")
    argparser.add_argument("--rechunk", action="store_true", default=False)
    argparser.add_argument("--use-distributed", action="store_true", default=False)
    args = argparser.parse_args()

    ds = main(
        fp_source_data=args.source_data_path,
        exp_name=args.experiment_name,
        data_kind=args.data_kind,
    )

    if args.rechunk:
        exp_name = args.experiment_name
        data_kind = args.data_kind
        fp_dest_data = args.source_data_path.parent / (
            args.source_data_path.name + "__zarr"
        )

        if args.use_distributed:
            client = dask.distributed.Client(n_workers=32)
            logger.info(f"Client: {client}")
            logger.info(f"Client URL: {client.dashboard_link}")

        target_chunks = dict(x=128, y=128, z=32, time=32)
        rechunk_to = dict()
        for dim in ds.dims:
            for dc in target_chunks:
                if dim.startswith(dc):
                    rechunk_to[dim] = target_chunks[dc]

        # rechunk and write to disk
        fp_temp = Path("/nwp/lcd/tmp")
        fn_zarr = f"{exp_name}.{data_kind}.zarr"
        fp_zarr = fp_dest_data / fn_zarr
        rechunk_and_write(ds=ds, fp_temp=fp_temp, fp_out=fp_zarr, rechunk_to=rechunk_to)
        logger.info(f"Zarr data written to `{fp_zarr}`")
