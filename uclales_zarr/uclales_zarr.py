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


def _subset_id(subset):
    return f"{subset[0]}_{subset[1]}"


def _drop_nonsplit_dims(dims):
    return [d for d in dims if d != "time" and not d.startswith("z")]


def _filter_chunks(refs, concat_dims):
    vars_to_keep = []

    for key, values in refs["refs"].items():
        if key.endswith("/.zattrs"):
            name, _ = key.split("/")
            if "_ARRAY_DIMENSIONS" in values:
                d = ujson.loads(values)
                dims = d["_ARRAY_DIMENSIONS"]
                var_concat_dims = frozenset(_drop_nonsplit_dims(dims))
                n_concat_dims = len(var_concat_dims)
                if n_concat_dims == 2:
                    if var_concat_dims == set(concat_dims):
                        vars_to_keep.append(name)
                elif n_concat_dims == 1:
                    if list(var_concat_dims)[0] in concat_dims:
                        vars_to_keep.append(name)
                else:
                    vars_to_keep.append(name)

    filtered_refs = {}
    for k, v in refs.items():
        if k != "refs":
            filtered_refs[k] = v
        else:
            filtered_refs[k] = {}
            for key, value in refs["refs"].items():
                if "/" in key:
                    name, _ = key.split("/")
                    if name in vars_to_keep:
                        filtered_refs[k][key] = value
                else:
                    filtered_refs[k][key] = value

    return filtered_refs


def _create_singlefile_zarr_jsons(path_src_jsons, fps_nc_files, is_netcdf4, subset):
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
            outf = f"{fp.name}.{_subset_id(subset)}.json"

            refs = h5chunks.translate()
            # TODO: filter refs by subset
            filtered_refs = _filter_chunks(refs=refs, concat_dims=subset)
            with fs.open(outf, "wb") as f:
                f.write(ujson.dumps(filtered_refs).encode())
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


def _multizarr_to_zarr(json_single_fps, dest_fpath_json, concat_dims):
    """
    Create a single json-description for a zarr-container (`dest_fpath_json`)
    from json-files for individual zarr files (`json_single_fps`)
    """
    logger.info(f"Writing single-file json zarr descriptor to `{dest_fpath_json}`")
    mzz = MultiZarrToZarr(json_single_fps, concat_dims=concat_dims)

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
    elif data_kind == "3d__first_2x3":
        fps_src = list(fp_source_data.glob(f"{exp_name}.000[0-1]000[0-2].nc"))
    elif data_kind in ["xy", "xz", "yz"]:
        fps_src = list(fp_source_data.glob(f"{exp_name}.out.{data_kind}.????.????.nc"))
    elif data_kind == "xy__first_2x3":
        fps_src = list(fp_source_data.glob(f"{exp_name}.out.xy.000[0-1].000[0-2].nc"))
    else:
        raise NotImplementedError(data_kind)

    logger.info(f"Found {len(fps_src)} source files")

    return fps_src


def _create_multizarr(exp_name, data_kind, fp_dest_data, fp_source_data, subset):
    fp_dest_json = fp_dest_data / f"{exp_name}.{data_kind}.{_subset_id(subset)}.json"

    if fp_dest_json.exists():
        logger.info(f"Skipping creation of {fp_dest_json} as it already exists")
        return fp_dest_json

    # filepaths for source netcdf files
    fps_src = _find_source_files(
        fp_source_data=fp_source_data, data_kind=data_kind, exp_name=exp_name
    )

    # where to place jsons for individual source files
    dirname_src_jsons = f"src_jsons__{data_kind}"
    fp_src_jsons = fp_dest_data / dirname_src_jsons

    try:
        h5py.File(fps_src[0], "r")
        is_netcdf4 = True
    except OSError:
        is_netcdf4 = False

    fps_src_jsons = _create_singlefile_zarr_jsons(
        path_src_jsons=fp_src_jsons,
        fps_nc_files=fps_src,
        is_netcdf4=is_netcdf4,
        subset=subset,
    )

    fps_src_jsons = sorted(fps_src_jsons)

    # check we can open one of the single-file jsons without issue
    _open_single_json(filepath=fps_src_jsons[0])

    _multizarr_to_zarr(
        json_single_fps=[str(fp) for fp in fps_src_jsons],
        dest_fpath_json=fp_dest_json,
        concat_dims=subset,
    )

    return fp_dest_json


def main(fp_source_data, exp_name, data_kind):
    fp_source_data = Path(fp_source_data, data_kind=data_kind)
    fp_dest_data = fp_source_data.parent / (fp_source_data.name + "__zarr")

    if data_kind.startswith("3d"):
        subsets = [("xt", "yt"), ("xt", "ym"), ("xm", "yt")]
    else:
        subsets = [("xt", "yt")]

    datasets = []
    for subset in tqdm(subsets, desc="Creating zarr files"):
        fp_dest_json = _create_multizarr(
            exp_name=exp_name,
            data_kind=data_kind,
            fp_dest_data=fp_dest_data,
            fp_source_data=fp_source_data,
            subset=subset,
        )

        # attempt to read the final json-file which describes all the source netcdf files
        ds = _read_multizarr(rpath=fp_dest_json)

        datasets.append(ds)

    if len(datasets) == 1:
        ds = datasets[0]
    else:
        ds = xr.merge(datasets, compat="override")

    logger.info(ds)
    return ds
