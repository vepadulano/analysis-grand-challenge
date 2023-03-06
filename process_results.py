import csv
import json
import math
import os

BENCHMARK_DIRS = ["coffea", "rdf-distributed", "rdf-imt"]


def main():

    for benchmark_dir in BENCHMARK_DIRS:
        for path in os.listdir(benchmark_dir):
            if "results-" in path:
                dirpath = os.path.join(benchmark_dir, path)
                print(dirpath)
                csvpath = os.path.join(dirpath, "summarytable.csv")
                with open(csvpath, "w") as csvfile:
                    csvwriter = csv.writer(csvfile)
                    csvwriter.writerow(["ncores", "wallclock_time_s", "read_rate_MB_s",
                                       "read_rate_MiB_s", "total_read_data_GB", "total_read_data_GiB"])

                jsondir = os.path.join(dirpath, "prmon-json")
                jsonfiles = [name for name in os.listdir(jsondir)]
                jsonfiles.sort()
                for name in jsonfiles:

                    full_path = os.path.join(jsondir, name)
                    with open(full_path) as jsonfile, open(csvpath, "a") as csvfile:
                        csvwriter = csv.writer(csvfile)
                        jsonthings = json.load(jsonfile)
                        maxvalues = jsonthings["Max"]
                        ncores = name.split("cores")[0]
                        total_read_data_GB = maxvalues["rx_bytes"] / 1e9
                        total_read_data_GiB = maxvalues["rx_bytes"] / math.pow(1024, 3)
                        wallclock_time_s = maxvalues["wtime"]
                        read_rate_MB_s = maxvalues["rx_bytes"] / wallclock_time_s / 1e6
                        read_rate_MiB_s = maxvalues["rx_bytes"] / wallclock_time_s / math.pow(1024, 2)
                        csvwriter.writerow([ncores, wallclock_time_s, read_rate_MB_s,
                                           read_rate_MiB_s, total_read_data_GB, total_read_data_GiB])


if __name__ == "__main__":
    raise SystemExit(main())
