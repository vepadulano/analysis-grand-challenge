from dask.distributed import LocalCluster, Client
import ROOT

# Point RDataFrame calls to Dask RDataFrame object
RDataFrame = ROOT.RDF.Experimental.Distributed.Dask.RDataFrame


def create_connection():
    cluster = LocalCluster(n_workers=2, threads_per_worker=1, processes=True)
    client = Client(cluster)
    return client


if __name__ == "__main__":

    # Create the connection to the mock Dask cluster on the local machine
    connection = create_connection()
    # Create an RDataFrame that will use Dask as a backend for computations
    # rdfentry needs to be converted to a column first
    df = RDataFrame(10, daskclient=connection).Define("x", "rdfentry_")
    df = df.Define("x2", lambda y: y * y, ["x"])
    sums = [df.Sum(col) for col in ["x", "x2"]]

    for s in sums:
        print(s.GetValue())
