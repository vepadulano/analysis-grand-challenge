# Benchmarks using the Analysis Grand Challenge to test performance of reading data from EOS

## Hardware setup

* AMD EPYC 7702P 64-Core Processor (1 socket, 2 threads per core)
* 100 Gbit/s connection
* 130 GB RAM

## Software setup

The environments to run the benchmarks are created via conda. To make
installation faster, I use [mamba](https://github.com/conda-forge/miniforge#mambaforge).
The `create_conda_env*` scripts can be used to create the environments.

## Analysis

The analysis is the CMS ttbar example, taken at the status of commit

https://github.com/iris-hep/analysis-grand-challenge/commit/c0b7e781102376dc5c9284b8574b29bc99158f17

The RDF version is taken from https://github.com/andriiknu/RDF/ ,adapted for
distributed case.

## How to run the benchmarks

The `run_all.sh` script can be used to run all the benchmarks in all folders.
In each folder, the benchmarks are first executed on the original dataset and
then on the merged dataset. Summary tables will be created in each directory,
inside the `results-*` sub-folders, after all benchmarks have run.
