# Rust MARC XML / Breaker / Binary Library

## Synopsis

See [Tests](./tests/record.rs) for examples.

## About

MARC Library for translating to/from MARC XML, MARC Breaker, and Binary MARC.

### Strings vs. Bytes?

For ease of use, the API primarily traffics in Strings instead of Bytes.
Bytes are used when reading/writing binary data and when confirming
valid byte-counts for codes/indicators/tags/leaders, but otherwise data
is stored and presented as Rust UTF-8 Strings.

This is especially handy for users accustomed to scripting languages,
where differentiating bytes, chars, and strings is not typically needed.

## Data Requirements

1. Data must be UTF-8 compatible.
1. Indicators and subfield codes must have a byte length of 1.
1. Tags must have a byte length of 3.
1. Leaders must have a byte length of 24.
1. Binary leader/directory metadata must be usable.

In cases where these conditions are not met, routines exit early with
explanatory Err() strings.

Otherwise, no restrictions are placed on the data values.
