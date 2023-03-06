# Runs the analysis in the directory $1 for all configurations

if [ -z "$1" ]; then
  echo "Supply a directory as first argument. Accepted values:"
  echo "'coffea', 'rdf-distributed', 'rdf-imt'"
  exit 1
fi

cd $1

export EXTRA_CLING_ARGS="-O2"

# First run on the original dataset
mkdir prmon-{plots,json,txt}
for i in 2 4 8 16 32 64; do
  prmon -i 1 -- python ttbar.py --ncores $i -l cern-xrootd

  prmon_plot.py --input prmon.txt --xvar wtime --yvar rx_bytes --diff --yunit MB
  prmon_plot.py --input prmon.txt --xvar wtime --yvar utime,stime --yunit SEC --diff --stacked
  prmon_plot.py --input prmon.txt --xvar wtime --yvar vmem,pss,rss,swap --yunit GB

  if [ "$i" -lt "10" ]; then
    # This makes sure the output files are labeled with a padded 2 digits number.
    # Makes post-processing easier.
    ncores=0$i
  else
    ncores=$i
  fi

  mv PrMon_wtime_vs_diff_rx_bytes.png prmon-plots/PrMon_wtime_vs_diff_rx_bytes_${ncores}cores.png
  mv PrMon_wtime_vs_diff_utime_stime.png prmon-plots/PrMon_wtime_vs_diff_utime_stime_${ncores}cores.png
  mv PrMon_wtime_vs_vmem_pss_rss_swap.png prmon-plots/PrMon_wtime_vs_vmem_pss_rss_swap_${ncores}cores.png

  mv prmon.txt prmon-txt/${ncores}cores.txt
  mv prmon.json prmon-json/${ncores}cores.json
done
mkdir results-nonmerged
mv prmon-* results-nonmerged

# And now on the merged one
mkdir prmon-{plots,json,txt}
for i in 2 4 8 16 32 64; do
  prmon -i 1 -- python ttbar.py --ncores $i -l cern-xrootd --merged-dataset

  prmon_plot.py --input prmon.txt --xvar wtime --yvar rx_bytes --diff --yunit MB
  prmon_plot.py --input prmon.txt --xvar wtime --yvar utime,stime --yunit SEC --diff --stacked
  prmon_plot.py --input prmon.txt --xvar wtime --yvar vmem,pss,rss,swap --yunit GB

  if [ "$i" -lt "10" ]; then
    # This makes sure the output files are labeled with a padded 2 digits number.
    # Makes post-processing easier.
    ncores=0$i
  else
    ncores=$i
  fi

  mv PrMon_wtime_vs_diff_rx_bytes.png prmon-plots/PrMon_wtime_vs_diff_rx_bytes_${ncores}cores.png
  mv PrMon_wtime_vs_diff_utime_stime.png prmon-plots/PrMon_wtime_vs_diff_utime_stime_${ncores}cores.png
  mv PrMon_wtime_vs_vmem_pss_rss_swap.png prmon-plots/PrMon_wtime_vs_vmem_pss_rss_swap_${ncores}cores.png

  mv prmon.txt prmon-txt/${ncores}cores.txt
  mv prmon.json prmon-json/${ncores}cores.json
done
mkdir results-merged
mv prmon-* results-merged
