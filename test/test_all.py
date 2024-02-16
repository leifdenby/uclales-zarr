from pathlib import Path

import pytest

from uclales_zarr import uclales_zarr


@pytest.mark.parametrize("data_kind", ["3d__first_2x3", "xy__first_2x3"])
def test_generate(data_kind):
    fp_src = Path(__file__).parent.parent / "testdata"
    exp_name = "rico_gcss"

    ds = uclales_zarr.main(
        fp_source_data=fp_src, exp_name=exp_name, data_kind=data_kind
    )

    nx, ny = 2, 3
    block_size = 32

    assert ds.xt.size == nx * block_size
    assert ds.yt.size == ny * block_size
    if "xm" in ds.dims:
        assert ds.xm.size == nx * block_size
    if "ym" in ds.dims:
        assert ds.ym.size == ny * block_size
