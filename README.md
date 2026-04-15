# PubChem Molecular Topology

[![CI](https://github.com/earth-metabolome-initiative/pubchem-topology/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/earth-metabolome-initiative/pubchem-topology/actions/workflows/ci.yml)
[![Codecov](https://codecov.io/gh/earth-metabolome-initiative/pubchem-topology/graph/badge.svg)](https://codecov.io/gh/earth-metabolome-initiative/pubchem-topology)
[![Zenodo](https://zenodo.org/badge/DOI/10.5281/zenodo.19599330.svg)](https://doi.org/10.5281/zenodo.19599330)
[![Rust 1.86+](https://img.shields.io/badge/rust-1.86%2B-93450a?logo=rust)](https://www.rust-lang.org)
[![License: MIT](https://img.shields.io/badge/license-MIT-yellow.svg)](LICENSE)

Download the current PubChem [`CID-SMILES.gz`](https://ftp.ncbi.nlm.nih.gov/pubchem/Compound/Extras/) release, parse it with [`smiles-parser`](https://github.com/earth-metabolome-initiative/smiles-parser), classify molecular topology with [`geometric-traits`](https://github.com/earth-metabolome-initiative/geometric-traits), write one row per CID to Parquet, write a JSON summary and SVG infographic, and publish the artifacts to Zenodo with `zenodo-rs`.

![PubChem Molecular Topology infographic](results/pubchem-topology-infographic.svg)

Reproduce the results with:

```bash
cp env.example .env
# set ZENODO_TOKEN in .env
RUSTFLAGS="-C target-cpu=native" cargo run --release
```
