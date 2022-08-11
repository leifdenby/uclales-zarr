import fsspec
import xarray as xr


def load(fp_zarr_json):
    fs = fsspec.filesystem(
        "reference",
        fo=fp_zarr_json,
    )
    m = fs.get_mapper("")
    ds = xr.open_dataset(
        m, engine="zarr", decode_times=False, chunks={}, consolidated=False
    )
    return ds
