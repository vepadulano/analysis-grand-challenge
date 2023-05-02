#!/bin/bash

echo "ROOT Installation: " `which root.exe`
echo "Python executable: " `which python`

parsed_hostnames=`scontrol show hostnames $SLURM_JOB_NODELIST | tr '\n' ',' | sed 's/.$//'`
ntests=5
nnodes=$RDF_JOB_NNODES
ncorespernode=$RDF_JOB_NCORESPERNODE

for npartitions_per_core in 1
do


npartitions=$(( nnodes * ncorespernode * npartitions_per_core ))

echo "Running $ntests tests with:"
echo "- Hosts: ${parsed_hostnames}"
echo "- cores per node: $ncorespernode"
echo "- npartitions (total): $npartitions"
echo "- ntests: $ntests"

python $PWD/ttbar.py "${parsed_hostnames}" $ncorespernode $npartitions -l cern-xrootd --merged-dataset --ntests $ntests

done

