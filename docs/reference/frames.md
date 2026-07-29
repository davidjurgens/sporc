# DataFrames & schema

The columnar API. See the [DataFrames guide](../guides/dataframes.md) for how
these fit together and for the traps they exist to remove.

Most of this surface is reached as methods on
[`SPORCDataset`](sporcdataset.md): `turns_frame`, `episodes_frame`,
`podcasts_frame`, `episode_metrics_frame`, `window_frame`, `iter_turns_frames`,
`turns_dataset`, `catalog`, `columns` and `parquet_paths`. The functions below
are the implementations, plus the pieces that work without a dataset:
`load_catalog`, `describe_columns`, `add_speaker_columns`,
`parse_episode_dates` and `window_frame_from_turns`.

## sporc.frames

::: sporc.frames
    options:
      show_root_heading: false
      members_order: source
      filters: ["!^_"]

## sporc.schema

What each table and column is, held as static data with no I/O, no pandas and
no credentials. This is what validates `columns=`, what estimates a request's
memory cost, and what `sporc columns` prints.

::: sporc.schema
    options:
      show_root_heading: false
      members_order: source
      filters: ["!^_"]
