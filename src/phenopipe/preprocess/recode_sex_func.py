import polars as pl

def recode_sex(input_dat):
    dat = input_dat.with_columns(
        pl.col("sex").replace_strict({"Male":"Male", "Female":"Female"}, default = None)
    )
    return dat.filter(pl.col("sex").is_not_null())