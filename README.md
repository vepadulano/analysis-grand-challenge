# Benchmarks of the Analysis Grand Challenge implemented with RDataFrame

## Software setup

The environments to run the benchmarks are created via conda. To make
installation faster, I use [mamba](https://github.com/conda-forge/miniforge#mambaforge).
The `create_conda_env*` scripts can be used to create the environments.

## Analysis

The analysis is the CMS ttbar example, taken at the status of commit

https://github.com/iris-hep/analysis-grand-challenge/commit/c0b7e781102376dc5c9284b8574b29bc99158f17

The RDF version is taken from https://github.com/andriiknu/RDF/ ,adapted for
distributed case.

## Categories

Different folders correspond to different configurations of the benchmark. The various configurations include
multithreading/distributed, traditional RDF with strings containing C++ code as well as pythonized interface.
