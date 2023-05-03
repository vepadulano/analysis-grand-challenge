from dask.distributed import LocalCluster, Client, WorkerPlugin
import ROOT

# Point RDataFrame calls to Dask RDataFrame object
RDataFrame = ROOT.RDF.Experimental.Distributed.Dask.RDataFrame
initialize = ROOT.RDF.Experimental.Distributed.initialize


def myinit():
    ROOT.gInterpreter.Declare("""
    #ifndef MYDEF
    #define MYDEF
    struct MyFunctor
    {
        ULong64_t operator()(ULong64_t l) { return l*l; };
    };
    #endif
    """)


class MyPlugin(WorkerPlugin):
    def setup(self, worker):
        myinit()


def create_connection():
    cluster = LocalCluster(n_workers=2, threads_per_worker=1, processes=True)
    client = Client(cluster)
    client.register_worker_plugin(MyPlugin())
    return client


if __name__ == "__main__":

    # Create the connection to the mock Dask cluster on the local machine
    connection = create_connection()

    myinit()
    initialize(myinit)
    f = ROOT.MyFunctor()

    rdf = RDataFrame(10, daskclient=connection)
    rdf2 = rdf.Define("x", f, ["rdfentry_"])

    print(rdf2.Sum("x").GetValue())
