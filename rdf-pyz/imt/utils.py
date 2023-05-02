import ROOT

ROOT.gInterpreter.Declare("""
struct RVecPxPyPzMVectorBuilder
{
    ROOT::RVec<ROOT::Math::PxPyPzMVector> operator()(
        const ROOT::RVecF &xs,
        const ROOT::RVecF &ys,
        const ROOT::RVecF &zs,
        const ROOT::RVecF &ms,
        const ROOT::RVecB &mask)
    {
        return ROOT::VecOps::Construct<ROOT::Math::PxPyPzMVector>(xs[mask], ys[mask], zs[mask], ms[mask]);
    }
};

struct CombinationsBuilder{

    ROOT::RVec<ROOT::RVec<std::size_t>> operator() (
        const ROOT::RVecF &v,
        const ROOT::RVecB &mask)
    {
        return ROOT::VecOps::Combinations(v[mask], 3);
    }
    
};

""")
build_jetp4 = ROOT.RVecPxPyPzMVectorBuilder()
combinations = ROOT.CombinationsBuilder()
