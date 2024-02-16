from .uclales_zarr import main


def load(src_fp, exp_name, data_kind):
    """
    Load the zarr data from the source filepath.

    Parameters
    ----------
    src_fp : str
        The source filepath, for example `/dmidata/projects/cloudphysics/les/rico_raw_data_2048
    exp_name : str
        The experiment name, for example `rico_gcss`.
    data_kind : str
        The data kind, for example `3d`, `xy` or `xz`.
    """
    return main(fp_source_data=src_fp, exp_name=exp_name, data_kind=data_kind)
