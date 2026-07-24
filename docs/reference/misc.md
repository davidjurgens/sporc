# Exceptions & constants

## Exceptions

Every error raised by the package derives from `SPORCError`, so a single
`except SPORCError` catches them all.

::: sporc.exceptions
    options:
      show_root_heading: false
      members_order: source

## Data sources

::: sporc.source
    options:
      show_root_heading: false
      members_order: source
      filters: ["!^_"]

## Category constants & helpers

The Apple Podcast category hierarchy and helpers for validating and expanding
categories. See the [Categories guide](../guides/categories.md).

::: sporc.constants
    options:
      show_root_heading: false
      members_order: source
      filters: ["!^_", "!^[A-Z_]+$"]
