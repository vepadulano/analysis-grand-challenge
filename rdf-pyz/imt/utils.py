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

struct NTriJetBuilder{

  std::size_t operator()(const ROOT::RVec<ROOT::RVec<std::size_t>> &trijet){return trijet[0].size();}
};

struct TriJetP4Builder{

    ROOT::RVec<ROOT::Math::PxPyPzMVector> operator()(
        const ROOT::RVec<ROOT::Math::PxPyPzMVector> &jet_p4,
        const ROOT::RVec<ROOT::RVec<std::size_t>> &trijet,
        std::size_t ntrijet)
    {
        ROOT::RVec<ROOT::Math::PxPyPzMVector> trijet_p4(ntrijet);
        for (int i = 0; i < ntrijet; ++i) {
            int j1 = trijet[0][i];
            int j2 = trijet[1][i];
            int j3 = trijet[2][i];
            trijet_p4[i] = jet_p4[j1] + jet_p4[j2] + jet_p4[j3];
        }
        return trijet_p4;
    }

};

struct TriJetPtBuilder{

    ROOT::RVecF operator()(const ROOT::RVec<ROOT::Math::PxPyPzMVector> &trijet_p4){
        return ROOT::VecOps::Map(trijet_p4, [](const ROOT::Math::PxPyPzMVector &v) { return v.Pt(); });
    }
};

struct TriJetBTagBuilder{

    ROOT::RVecB operator()(std::size_t ntrijet, const ROOT::RVec<ROOT::RVec<std::size_t>> &trijet, const ROOT::RVecD &jet_btag){
        ROOT::VecOps::RVec<bool> btag(ntrijet);
        for (int i = 0; i < ntrijet; ++i) {
            int j1 = trijet[0][i];
            int j2 = trijet[1][i];
            int j3 = trijet[2][i];
            btag[i] = std::max({jet_btag[j1], jet_btag[j2], jet_btag[j3]}) > 0.5;
        }
        return btag;
    }
};

struct ObservableBuilder{

    double operator()(std::size_t ntrijet,
        const ROOT::RVecF &trijet_pt,
        const ROOT::RVecB &trijet_btag,
        const ROOT::RVec<ROOT::Math::PxPyPzMVector> &trijet_p4)
    {
        double mass{};
        double pt{};
        double indx{};
        for (int i = 0; i < ntrijet; ++i) {
            if ((pt < trijet_pt[i]) && (trijet_btag[i])) {
                pt = trijet_pt[i];
                indx = i;
            }
        }
        mass = trijet_p4[indx].M();
        return mass;
    }
};
""")

build_jetp4 = ROOT.RVecPxPyPzMVectorBuilder()
combinations = ROOT.CombinationsBuilder()
build_trijetp4 = ROOT.TriJetP4Builder()
build_trijetpt = ROOT.TriJetPtBuilder()
build_trijetbtag = ROOT.TriJetBTagBuilder()
get_ntrijet = ROOT.NTriJetBuilder()
build_observable = ROOT.ObservableBuilder()
