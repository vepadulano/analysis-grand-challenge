#include "TH1D.h"
#include <string>
#include "TRandom3.h"
#include "ROOT/RVec.hxx"
#include "Math/Vector4D.h"

// functions slicing histograms

// accept numbers of bins as parameters
TH1D SliceHisto(TH1D h, int xfirst, int xlast)
{

    // do slice in xfirst:xlast including xfirst and xlast
    TH1D res((std::string("h_sliced_") + h.GetTitle()).c_str(), h.GetTitle(), xlast - xfirst, h.GetXaxis()->GetBinLowEdge(xfirst), h.GetXaxis()->GetBinUpEdge(xlast - 1));
    // note that histogram arrays are : [ undeflow, bin1, bin2,....., binN, overflow]
    std::copy(h.GetArray() + xfirst, h.GetArray() + xlast, res.GetArray() + 1);
    // set correct underflow/overflows
    res.SetBinContent(0, h.Integral(0, xfirst - 1));                              // set underflow value
    res.SetBinContent(res.GetNbinsX() + 1, h.Integral(xlast, h.GetNbinsX() + 1)); // set overflow value

    return res;
}

// accept axis limits as parameters
TH1D Slice(TH1D h, double low_edge, double high_edge)
{
    int xfirst = h.FindBin(low_edge);
    int xlast = h.FindBin(high_edge);
    return SliceHisto(h, xfirst, xlast);
}

// functions creating systematic variations
struct pt_res_up
{
    std::vector<TRandom> fRandom;
    pt_res_up(unsigned nSlots) : fRandom(nSlots)
    {
        for (int i = 0; i < nSlots; ++i)
            fRandom[i].SetSeed(gRandom->Integer(1000));
    }

    ROOT::VecOps::RVec<float> operator()(const ROOT::VecOps::RVec<float> &jet_pt, unsigned int slot)
    {
        // normal distribution with 5% variations, shape matches jets
        ROOT::VecOps::RVec<float> res(jet_pt.size());
        for (auto &e : res)
        {
            e = fRandom[slot].Gaus(1, 0.05);
        }
        return res;
    }
};

inline TRandom &get_thread_local_trandom()
{
  thread_local TRandom rng;
  rng.SetSeed(gRandom->Integer(1000));
  return rng;
}

ROOT::VecOps::RVec<float> jet_pt_resolution(const ROOT::VecOps::RVec<float> &jet_pt)
{
    // normal distribution with 5% variations, shape matches jets
    ROOT::VecOps::RVec<float> res(jet_pt.size());
    for (auto &e : res)
    {
        e = get_thread_local_trandom().Gaus(1, 0.05);
    }
    return res;
}

float pt_scale_up()
{
    return 1.03;
}

ROOT::VecOps::RVec<float> btag_weight_variation(const ROOT::VecOps::RVec<float> &jet_pt)
{
    // weight variation depending on i-th jet pT (7.5% as default value, multiplied by i-th jet pT / 50 GeV)
    ROOT::VecOps::RVec<float> res;
    for (const float &pt : ROOT::VecOps::Take(jet_pt, 4))
    {
        res.push_back(1 + .075 * pt / 50);
        res.push_back(1 - .075 * pt / 50);
    }
    return res;
}

ROOT::VecOps::RVec<float> flat_variation()
{
    // 2.5% weight variationss
    return ROOT::VecOps::RVec<float>({1.025, 0.975});
}

struct VariedWeights
{
    ROOT::VecOps::RVec<double> operator()(double w)
    {
        return w * flat_variation();
    }
};

struct JetPtVariationBuilder
{

    ROOT::RVec<ROOT::RVecF> operator()(
        const ROOT::RVecF &jet_pt)
    {
        return {jet_pt * pt_scale_up(), jet_pt * jet_pt_resolution(jet_pt)};
    }
};

struct BTagVariationBuilder
{

    ROOT::RVecD operator()(
        const ROOT::RVecF &jet_pt,
        const ROOT::RVecB &jet_pt_mask,
        double w)
    {
        return {w * btag_weight_variation(jet_pt[jet_pt_mask])};
    }
};

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

struct CombinationsBuilder
{

    ROOT::RVec<ROOT::RVec<std::size_t>> operator()(
        const ROOT::RVecF &v,
        const ROOT::RVecB &mask)
    {
        return ROOT::VecOps::Combinations(v[mask], 3);
    }
};

struct NTriJetBuilder
{

    std::size_t operator()(const ROOT::RVec<ROOT::RVec<std::size_t>> &trijet) { return trijet[0].size(); }
};

struct TriJetP4Builder
{

    ROOT::RVec<ROOT::Math::PxPyPzMVector> operator()(
        const ROOT::RVec<ROOT::Math::PxPyPzMVector> &jet_p4,
        const ROOT::RVec<ROOT::RVec<std::size_t>> &trijet,
        std::size_t ntrijet)
    {
        ROOT::RVec<ROOT::Math::PxPyPzMVector> trijet_p4(ntrijet);
        for (int i = 0; i < ntrijet; ++i)
        {
            int j1 = trijet[0][i];
            int j2 = trijet[1][i];
            int j3 = trijet[2][i];
            trijet_p4[i] = jet_p4[j1] + jet_p4[j2] + jet_p4[j3];
        }
        return trijet_p4;
    }
};

struct TriJetPtBuilder
{

    ROOT::RVecF operator()(const ROOT::RVec<ROOT::Math::PxPyPzMVector> &trijet_p4)
    {
        return ROOT::VecOps::Map(trijet_p4, [](const ROOT::Math::PxPyPzMVector &v)
                                 { return v.Pt(); });
    }
};

struct TriJetBTagBuilder
{

    ROOT::RVecB operator()(std::size_t ntrijet, const ROOT::RVec<ROOT::RVec<std::size_t>> &trijet, const ROOT::RVecD &jet_btag)
    {
        ROOT::VecOps::RVec<bool> btag(ntrijet);
        for (int i = 0; i < ntrijet; ++i)
        {
            int j1 = trijet[0][i];
            int j2 = trijet[1][i];
            int j3 = trijet[2][i];
            btag[i] = std::max({jet_btag[j1], jet_btag[j2], jet_btag[j3]}) > 0.5;
        }
        return btag;
    }
};

struct ObservableBuilder
{

    double operator()(std::size_t ntrijet,
                      const ROOT::RVecF &trijet_pt,
                      const ROOT::RVecB &trijet_btag,
                      const ROOT::RVec<ROOT::Math::PxPyPzMVector> &trijet_p4)
    {
        double mass{};
        double pt{};
        double indx{};
        for (int i = 0; i < ntrijet; ++i)
        {
            if ((pt < trijet_pt[i]) && (trijet_btag[i]))
            {
                pt = trijet_pt[i];
                indx = i;
            }
        }
        mass = trijet_p4[indx].M();
        return mass;
    }
};