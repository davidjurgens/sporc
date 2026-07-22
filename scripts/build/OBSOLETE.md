# Scripts kept only as a record

`rename_search_column.py.obsolete`
`repair_search_fts.py.obsolete`

These renamed `turns_search.turns.word_count` to `token_count`, and then
repaired the full-text index that the rename had destroyed.

The rename was wrong. In the Parquet tree, `turns/text.word_count` really did
hold aligned token counts under a misleading name, and renaming it was right.
The search database is not that column: `stage_search.py` computes its
`word_count` from spaces in the turn text, so it was already a word count and
already correctly named.

Do not run these against the current database. `stage_search.py` builds it with
the right name, and the 2026-07-22 rebuild did exactly that.
