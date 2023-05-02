import ROOT

rdf = ROOT.RDataFrame(5).Define("x", "rdfentry_") # rdfentry needs to be converted to a column first
rdf = rdf.Define("x2", lambda y: y*y, ["x"]).Display().Print()
