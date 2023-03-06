for dirname in "coffea" "rdf-distributed" "rdf-imt"
do
    bash run_benchmark_dir.sh $dirname
done

# Create summary tables for all results
python process_results.py
