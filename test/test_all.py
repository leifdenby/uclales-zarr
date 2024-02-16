from pathlib import Path

from uclales_zarr import uclales_zarr


def test_generate():
    data_kind = "3d__first_2x3"

    fp_src = Path(__file__).parent.parent / "testdata"
    exp_name = "rico_gcss"

    ds = uclales_zarr.main(
        fp_source_data=fp_src, exp_name=exp_name, data_kind=data_kind
    )

    nx, ny = 2, 3
    block_size = 32

    assert ds.xt.size == nx * block_size
    assert ds.yt.size == ny * block_size
    assert ds.xm.size == nx * block_size
    assert ds.ym.size == ny * block_size
