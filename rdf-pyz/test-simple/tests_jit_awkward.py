import numpy
import awkward
import numba

@numba.njit
def jet_pt_resolution(pt):
    # normal distribution with 5% variations, shape matches jets
    counts = awkward.num(pt)
    pt_flat = awkward.flatten(pt)
    resolution_variation = numpy.random.normal(numpy.ones_like(pt_flat), 0.05)
    return awkward.unflatten(resolution_variation, counts)

jet_pt_resolution([1.,2.,3.,4.])