#!/usr/bin/env python
# coding: utf-8

from pathlib import Path

import dask
import fsspec
import fsspec.implementations
import fsspec.implementations.dirfs
import ujson  # fast json
import xarray as xr
from dask.diagnostics import ProgressBar
from kerchunk.combine import MultiZarrToZarr
from kerchunk.hdf import SingleHdf5ToZarr
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


def _create_singlefile_zarr_jsons(path_src_jsons, fps_nc4_files):
    logger.info("Creating JSON file for each individual source NetCDF4 file")
    path_src_jsons = Path(path_src_jsons)
    path_src_jsons.mkdir(exist_ok=True, parents=True)
    fs = fsspec.implementations.dirfs.DirFileSystem(
        path=path_src_jsons,
        fs=fsspec.filesystem("file", auto_mkdir=True),
    )

    def gen_json(fp):
        with fsspec.open(fp) as infile:
            h5chunks = SingleHdf5ToZarr(infile, str(fp), inline_threshold=300)
            outf = f"{fp.name}.json"
            with fs.open(outf, "wb") as f:
                f.write(ujson.dumps(h5chunks.translate()).encode())
            return Path(fs.path) / outf

    files = fps_nc4_files
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


def main(fp_source_data, exp_name):
    fp_source_data = Path(fp_source_data)
    fp_dest_data = fp_source_data.parent / (fp_source_data.name + "__zarr")

    fps_src = list(fp_source_data.glob(f"{exp_name}.000?000?.nc"))
    logger.info(f"Found {len(fps_src)} soure files")

    fps_nc4_files = _convert_nc3_to_nc4(fps_src=fps_src, dest_path=fp_dest_data / "nc4")
    fps_src_jsons = _create_singlefile_zarr_jsons(
        path_src_jsons=fp_dest_data / "src_jsons", fps_nc4_files=fps_nc4_files
    )
    # check we can open one of the single-file jsons without issue
    _open_single_json(filepath=fps_src_jsons[0])

    fp_dest_json = fp_dest_data / f"{exp_name}.json"
    _multizarr_to_zarr(json_single_fps=fps_src_jsons, dest_fpath_json=fp_dest_json)

    # attempt to read the final json-file which describes all the source netcdf files
    ds = _read_multizarr(rpath=fp_dest_json)
    logger.info(ds)
