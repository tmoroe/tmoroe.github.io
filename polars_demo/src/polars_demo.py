import polars as pl
import polars.selectors as cs

pl.Config.set_fmt_str_lengths(900)
pl.Config.set_tbl_width_chars(900)

# the header column names are both in row 1 and 2, ignore null row and use populated row.
df = pl.read_excel("../data/sales_data.xlsx")
renames = {k: v for d in df.head(2).to_dicts() for k, v in d.items() if v}

# attach shipping mode to segment names to every column name. The shipping mode name appears after every 3 columns in the first row.
segment = ""
for old,new in renames.items():
    segment = old if "duplicated" not in old else segment
    renames[old] = f"{new}_{segment}".replace(" ", "_").lower()

#apply the renaming dict
df = df.rename(renames)

# cast the order date column to date datatype
df = df.with_columns(pl.col("order_date_ship_mode").str.to_date("%d-%b-%y", strict=False).alias("order_date")).drop("order_date_ship_mode")

# drop columns where order date is null 
df = df.filter(~pl.col("order_date").is_null())

# stack the different orders into order class and cost columns
df = df.melt(id_vars=["order_id_", "order_date"])

#now we can drop empty orders by filtering out null costs. The table is now usable.
df = df.filter(~pl.col("value").is_null())

# we will further split shipping mode and segment as they concatenated earlier. This will become useful to partition/index data at rest
# to optimize queries. We then drop the field after extraction. 

df = df.with_columns(
    pl.col("variable").str.extract(r".+_(\S+_\S+)$", 1).alias("shipping_mode"),
    pl.col("variable").str.extract(r"^(\S+_\S+)_.+_", 1).alias("segment")
).drop("variable")

# cast the order cost to a float to allow for arithmetic operations on the column.\
df = df.cast({"value":pl.Float64})

# The table is now clean and usable.
print(df.tail(50))
print(df.count())

# ┌────────────────┬────────────┬─────────┬────────────────┬─────────────┐
# │ order_id_      ┆ order_date ┆ value   ┆ shipping_mode  ┆ segment     │
# │ ---            ┆ ---        ┆ ---     ┆ ---            ┆ ---         │
# │ str            ┆ date       ┆ f64     ┆ str            ┆ str         │
# ╞════════════════╪════════════╪═════════╪════════════════╪═════════════╡
# │ CA-2013-159891 ┆ 2015-01-31 ┆ 1396.35 ┆ standard_class ┆ home_office │
# │ CA-2013-165470 ┆ 2015-11-26 ┆ 5.08    ┆ standard_class ┆ home_office │
# │ CA-2013-169026 ┆ 2015-08-09 ┆ 23.34   ┆ standard_class ┆ home_office │
# │ CA-2014-100412 ┆ 2016-12-22 ┆ 141.96  ┆ standard_class ┆ home_office │
# │ CA-2014-102554 ┆ 2016-06-11 ┆ 3.76    ┆ standard_class ┆ home_office │
# │ …              ┆ …          ┆ …       ┆ …              ┆ …           │
# │ US-2014-129224 ┆ 2016-03-17 ┆ 4.608   ┆ standard_class ┆ home_office │
# │ US-2014-132031 ┆ 2016-04-23 ┆ 513.496 ┆ standard_class ┆ home_office │
# │ US-2014-132297 ┆ 2016-05-27 ┆ 598.31  ┆ standard_class ┆ home_office │
# │ US-2014-132675 ┆ 2016-09-24 ┆ 148.16  ┆ standard_class ┆ home_office │
# │ US-2014-156083 ┆ 2016-11-04 ┆ 9.664   ┆ standard_class ┆ home_office │
# └────────────────┴────────────┴─────────┴────────────────┴─────────────┘