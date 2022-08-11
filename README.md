# zarr-json writer for UCLALES model output

Usage

```bash
python -m uclales_zarr.cli [source_data_path] [experiment_name]
```

This will write combined json-file that can be opened as zarr archive with in
path `{source_file_path}__zarr/{experiment_name}.json`. Because UCLALES writes
files in NetCDF3 format each source file will be convered to NetCDF4 (written
to `{source_file_path}__zarr/nc4`).

This can then be loaded in python with

```python
import uclales_zarr

source_file_path = "{change this}"
experiment_name = "{change this}"

ds = uclales_zarr.load(f"{source_file_path}__zarr/{experiment_name}.json")
# `ds` is now a regular xr.Dataset you can use to access the full simulation
# data as if it is a single file
```

## Implementation progress

- [x] conversion to NetCDF4 from NetCDF3 (may be able to use NetCDF3 natively
  as of `fsspec` `0.0.7`)

- [x] creation of individual json-files for 3D source files, **NB**: on fields at
  cell centers have correct coordinate (x, y, z) values for now. The staggered
  variables (velocities) need to be treated separately

- [ ] support for creating coordinate values for staggered fields (velocities)

- [ ] support for 2D cross-section fields

- [ ] try natively using NetCDF3 rather than converting, this appears to be
  possible now with `ffspec` `0.0.7`
  (https://github.com/fsspec/kerchunk/pull/190) although there might be
  degradation in access speeds.

- [ ] check whether UCLALES model output is actually already NetCDF4 so we can
  avoid conversion (will be part of UCLALES `2.1.0`)
