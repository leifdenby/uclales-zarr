#!/usr/bin/env python
# coding: utf-8

from pathlib import Path

import dask
import fsspec
import fsspec.implementations
import fsspec.implementations.dirfs
import h5py
import ujson  # fast json
import xarray as xr
from dask.diagnostics import ProgressBar
from kerchunk.combine import MultiZarrToZarr
from kerchunk.hdf import SingleHdf5ToZarr
from kerchunk.netCDF3 import NetCDF3ToZarr
from loguru import logger
from tqdm.auto import tqdm

# ensure we get progress-bar for dask.compute
ProgressBar().register()


def _convert_nc3_to_nc4(fps_src, dest_path):
    logger.info("Creating netcdf4 files from netcdf3 files")
    dest_path = Path(dest_path)
    dest_path.mkdir(exist_ok=True, parents=True)

    files_nc4 = []
    for fp in tqdm(fps_src):
        ds_src = xr.open_dataset(fp, decode_times=False)
        fp_dst = dest_path / fp.name
        if not fp_dst.exists():
            ds_src.to_netcdf(fp_dst)
        files_nc4.append(fp_dst)
    return files_nc4


def _create_singlefile_zarr_jsons(path_src_jsons, fps_nc_files, is_netcdf4=False):
    logger.info("Creating JSON file for each individual source NetCDF file")
    path_src_jsons = Path(path_src_jsons)
    path_src_jsons.mkdir(exist_ok=True, parents=True)
    fs = fsspec.implementations.dirfs.DirFileSystem(
        path=path_src_jsons,
        fs=fsspec.filesystem("file", auto_mkdir=True),
    )

    def gen_json(fp):
        with fsspec.open(fp) as infile:
            if is_netcdf4:
                h5chunks = SingleHdf5ToZarr(infile, str(fp), inline_threshold=300)
            else:
                h5chunks = NetCDF3ToZarr(str(fp))
            outf = f"{fp.name}.json"
            with fs.open(outf, "wb") as f:
                f.write(ujson.dumps(h5chunks.translate()).encode())
            return Path(fs.path) / outf

    files = fps_nc_files
    json_single_fps = dask.compute(
        *[dask.delayed(gen_json)(u) for u in files], retries=10
    )
    return json_single_fps


def _open_single_json(filepath):
    m = fsspec.get_mapper(
        "reference://",
        fo=str(filepath),
    )

    ds = xr.open_dataset(m, engine="zarr", decode_times=False, consolidated=False)
    return ds


def _multizarr_to_zarr(json_single_fps, dest_fpath_json):
    """
    Create a single json-description for a zarr-container (`dest_fpath_json`)
    from json-files for individual zarr files (`json_single_fps`)
    """
    logger.info(f"Writing single-file json zarr descriptor to `{dest_fpath_json}`")
    mzz = MultiZarrToZarr(
        json_single_fps,
        concat_dims=[
            "xt",
            "yt",
        ],
    )

    mzz.translate(dest_fpath_json)


def _read_multizarr(rpath):
    fs = fsspec.filesystem(
        "reference",
        fo=str(rpath),
    )
    m = fs.get_mapper("")
    ds = xr.open_dataset(
        m, engine="zarr", decode_times=False, chunks={}, consolidated=False
    )
    return ds


def _find_source_files(fp_source_data, data_kind, exp_name):
    if data_kind == "3d":
        fps_src = list(fp_source_data.glob(f"{exp_name}.????????.nc"))
    elif data_kind == "3d__first_10x10":
        fps_src = list(fp_source_data.glob(f"{exp_name}.000?000?.nc"))
    elif data_kind in ["xy", "xz", "yz"]:
        fps_src = list(fp_source_data.glob(f"{exp_name}.out.{data_kind}.????.????.nc"))
    elif data_kind == "xy__first_10x10":
        fps_src = list(fp_source_data.glob(f"{exp_name}.out.xy.000?.000?.nc"))
    else:
        raise NotImplementedError(data_kind)

    logger.info(f"Found {len(fps_src)} soure files")

    return fps_src


def main(fp_source_data, exp_name, data_kind, convert_to_nc4=False):
    fp_source_data = Path(fp_source_data, data_kind=data_kind)
    fp_dest_data = fp_source_data.parent / (fp_source_data.name + "__zarr")

    fps_src = _find_source_files(
        fp_source_data=fp_source_data, data_kind=data_kind, exp_name=exp_name
    )

    dirname_src_jsons = f"src_jsons__{data_kind}"
    if convert_to_nc4:
        dirname_src_jsons += "_nc4"

    fp_src_jsons = fp_dest_data / dirname_src_jsons
    if convert_to_nc4:
        fps_src_ready = _convert_nc3_to_nc4(
            fps_src=fps_src, dest_path=fp_dest_data / f"{data_kind}_nc4"
        )
    else:
        fps_src_ready = fps_src

    try:
        h5py.File(fps_src[0], "r")
        is_netcdf4 = True
    except OSError:
        is_netcdf4 = False

    fps_src_jsons = _create_singlefile_zarr_jsons(
        path_src_jsons=fp_src_jsons,
        fps_nc_files=fps_src_ready,
        is_netcdf4=is_netcdf4,
    )

    # check we can open one of the single-file jsons without issue
    _open_single_json(filepath=fps_src_jsons[0])

    fp_dest_json = fp_dest_data / f"{exp_name}.json"
    _multizarr_to_zarr(json_single_fps=fps_src_jsons, dest_fpath_json=fp_dest_json)

    # attempt to read the final json-file which describes all the source netcdf files
    ds = _read_multizarr(rpath=fp_dest_json)
    logger.info(ds)
