import argparse
import asyncio
import json
import logging
import os
import textwrap
import time

# Do this here, and things will go well
import vector
vector.register_awkward()

import numpy as np
import matplotlib.pyplot as plt

import awkward as ak
from coffea.nanoevents.schemas.base import BaseSchema, zip_forms
from coffea.nanoevents.methods import base, vector
from coffea.nanoevents import transforms
# Can't install the environment with pip on Python 3.11
# servicex is not available via conda either
# from coffea.processor import servicex
from coffea import processor
# Can't install the environment with pip on Python 3.11
# func-adl is not available via conda either
# from func_adl import ObjectStream
import hist
import utils  # contains code for bookkeeping and cosmetics, as well as some boilerplate


PARSER = argparse.ArgumentParser()
PARSER.add_argument("--ncores", help="Number of cores to use. Default os.cpu_count().",
                    type=int, default=os.cpu_count())
PARSER.add_argument("--n-files-max-per-sample", "-f",
                    help="How many files per sample will be processed. Default -1 (all files for all samples).",
                    type=int, default=-1)
PARSER.add_argument("--pipeline", "-p",
                    help=textwrap.dedent("""
                    pipeline to use:
                    - "coffea" for pure coffea setup
                    - "servicex_processor" for coffea with ServiceX processor
                    - "servicex_databinder" for downloading query output and subsequent standalone coffea
                    """),
                    default="coffea")
PARSER.add_argument("--storage-location", "-l",
                    help="Where the data resides. Default dataset is stored at UNL.", default="unl")
PARSER.add_argument("--histograms-output-file",
                    help="Name of the output file to store histograms.", default="histograms.root")
PARSER.add_argument("--merged-dataset", help="Use the merged dataset instead of the original one.", action="store_true")
PARSER.add_argument("--download", "-d", help="Download the files locally when executing.", action="store_true")
PARSER.add_argument("--use-dask",
                    help="enable Dask (may not work yet in combination with ServiceX outside of coffea-casa).",
                    action="store_true")
PARSER.add_argument("--servicex-ignore-cache",
                    help="ServiceX behavior: ignore cache with repeated queries.", action="store_true")
PARSER.add_argument("--analysis-facility",
                    help="Set to 'coffea_casa' for coffea-casa environments, 'EAF' for FNAL, 'local' for local setups",
                    default="local")
PARSER.add_argument("--disable-processing",
                    help="Run only I/O transactions, all other processing disabled.", action="store_true")
PARSER.add_argument("--io-file-percent",
                    help=textwrap.dedent("""
                    Read additional branches (only with --disable-processing option). Acceptable values are 4, 15, 25,
                    50 (corresponding to %% of file read), 4%% corresponds to the standard branches used in the notebook
                    """),
                    default=4,
                    choices=[4, 15, 25, 50],
                    type=int)
PARSER.add_argument('-v', '--verbose', action='store_true')
ARGS = PARSER.parse_args()

logging.getLogger("cabinetry").setLevel(logging.INFO)


# chunk size to use
CHUNKSIZE = 500_000

# metadata to propagate through to metrics
AF_NAME = "coffea_casa"  # "ssl-dev" allows for the switch to local data on /data
SYSTEMATICS = "all"  # currently has no effect
CORES_PER_WORKER = 2  # does not do anything, only used for metric gathering (set to 2 for distributed coffea-casa)
PROCESSOR_BASE = processor.ProcessorABC  # if (ARGS.pipeline != "servicex_processor") else servicex.Analysis

NTUPLES_FILE = "ntuples_merged.json" if ARGS.merged_dataset else "ntuples.json"

# functions creating systematic variations
def flat_variation(ones):
    # 2.5% weight variations
    return (1.0 + np.array([0.025, -0.025], dtype=np.float32)) * ones[:, None]


def btag_weight_variation(i_jet, jet_pt):
    # weight variation depending on i-th jet pT (7.5% as default value, multiplied by i-th jet pT / 50 GeV)
    return 1 + np.array([0.075, -0.075]) * (ak.singletons(jet_pt[:, i_jet]) / 50).to_numpy()


def jet_pt_resolution(pt):
    # normal distribution with 5% variations, shape matches jets
    counts = ak.num(pt)
    pt_flat = ak.flatten(pt)
    resolution_variation = np.random.normal(np.ones_like(pt_flat), 0.05)
    return ak.unflatten(resolution_variation, counts)


class TtbarAnalysis(PROCESSOR_BASE):
    def __init__(self, disable_processing, io_file_percent):
        num_bins = 25
        bin_low = 50
        bin_high = 550
        name = "observable"
        label = "observable [GeV]"
        self.hist = (
            hist.Hist.new.Reg(num_bins, bin_low, bin_high, name=name, label=label)
            .StrCat(["4j1b", "4j2b"], name="region", label="Region")
            .StrCat([], name="process", label="Process", growth=True)
            .StrCat([], name="variation", label="Systematic variation", growth=True)
            .Weight()
        )
        self.disable_processing = disable_processing
        self.io_file_percent = io_file_percent

    def only_do_IO(self, events):
        # standard AGC branches cover 4% of the data
        branches_to_read = ["jet_pt", "jet_eta", "jet_phi", "jet_btag", "jet_e", "muon_pt", "electron_pt"]
        if self.io_file_percent not in [4, 15, 25, 50]:
            raise NotImplementedError("supported values for I/O percentage are 4, 15, 25, 50")
        if self.io_file_percent >= 15:
            branches_to_read += ["trigobj_e"]
        if self.io_file_percent >= 25:
            branches_to_read += ["trigobj_pt"]
        if self.io_file_percent >= 50:
            branches_to_read += ["trigobj_eta", "trigobj_phi", "jet_px", "jet_py", "jet_pz", "jet_ch"]

        for branch in branches_to_read:
            if "_" in branch:
                object_type, property_name = branch.split("_")
                if property_name == "e":
                    property_name = "energy"
                ak.materialized(events[object_type][property_name])
            else:
                ak.materialized(events[branch])
        return {"hist": {}}

    def process(self, events):
        if self.disable_processing:
            # IO testing with no subsequent processing
            return self.only_do_IO(events)

        histogram = self.hist.copy()

        process = events.metadata["process"]  # "ttbar" etc.
        variation = events.metadata["variation"]  # "nominal" etc.

        # normalization for MC
        x_sec = events.metadata["xsec"]
        nevts_total = events.metadata["nevts"]
        lumi = 3378  # /pb
        if process != "data":
            xsec_weight = x_sec * lumi / nevts_total
        else:
            xsec_weight = 1

        # systematics
        # example of a simple flat weight variation, using the coffea nanoevents systematics feature
        if process == "wjets":
            events.add_systematic("scale_var", "UpDownSystematic", "weight", flat_variation)

        # jet energy scale / resolution systematics
        # need to adjust schema to instead use coffea add_systematic feature, especially for ServiceX
        # cannot attach pT variations to events.jet, so attach to events directly
        # and subsequently scale pT by these scale factors
        events["pt_nominal"] = 1.0
        events["pt_scale_up"] = 1.03
        events["pt_res_up"] = jet_pt_resolution(events.jet.pt)

        pt_variations = ["pt_nominal", "pt_scale_up", "pt_res_up"] if variation == "nominal" else ["pt_nominal"]
        for pt_var in pt_variations:

            # event selection
            # very very loosely based on https://arxiv.org/abs/2006.13076

            # pT > 25 GeV for leptons & jets
            selected_electrons = events.electron[events.electron.pt > 25]
            selected_muons = events.muon[events.muon.pt > 25]
            jet_filter = events.jet.pt * events[pt_var] > 25  # pT > 25 GeV for jets (scaled by systematic variations)
            selected_jets = events.jet[jet_filter]

            # single lepton requirement
            event_filters = ((ak.count(selected_electrons.pt, axis=1) + ak.count(selected_muons.pt, axis=1)) == 1)
            # at least four jets
            pt_var_modifier = events[pt_var] if "res" not in pt_var else events[pt_var][jet_filter]
            event_filters = event_filters & (ak.count(selected_jets.pt * pt_var_modifier, axis=1) >= 4)
            # at least one b-tagged jet ("tag" means score above threshold)
            B_TAG_THRESHOLD = 0.5
            event_filters = event_filters & (ak.sum(selected_jets.btag >= B_TAG_THRESHOLD, axis=1) >= 1)

            # apply event filters
            selected_events = events[event_filters]
            selected_electrons = selected_electrons[event_filters]
            selected_muons = selected_muons[event_filters]
            selected_jets = selected_jets[event_filters]

            for region in ["4j1b", "4j2b"]:
                # further filtering: 4j1b CR with single b-tag, 4j2b SR with two or more tags
                if region == "4j1b":
                    region_filter = ak.sum(selected_jets.btag >= B_TAG_THRESHOLD, axis=1) == 1
                    selected_jets_region = selected_jets[region_filter]
                    # use HT (scalar sum of jet pT) as observable
                    pt_var_modifier = (
                        events[event_filters][region_filter][pt_var]
                        if "res" not in pt_var
                        else events[pt_var][jet_filter][event_filters][region_filter]
                    )
                    observable = ak.sum(selected_jets_region.pt * pt_var_modifier, axis=-1)

                elif region == "4j2b":
                    region_filter = ak.sum(selected_jets.btag > B_TAG_THRESHOLD, axis=1) >= 2
                    selected_jets_region = selected_jets[region_filter]

                    # reconstruct hadronic top as bjj system with largest pT
                    # the jet energy scale / resolution effect is not propagated to this observable at the moment
                    trijet = ak.combinations(selected_jets_region, 3, fields=["j1", "j2", "j3"])  # trijet candidates
                    trijet["p4"] = trijet.j1 + trijet.j2 + trijet.j3  # calculate four-momentum of tri-jet system
                    trijet["max_btag"] = np.maximum(trijet.j1.btag, np.maximum(trijet.j2.btag, trijet.j3.btag))
                    trijet = trijet[trijet.max_btag > B_TAG_THRESHOLD]  # at least one-btag in trijet candidates
                    # pick trijet candidate with largest pT and calculate mass of system
                    trijet_mass = trijet["p4"][ak.argmax(trijet.p4.pt, axis=1, keepdims=True)].mass
                    observable = ak.flatten(trijet_mass)

                # histogram filling
                if pt_var == "pt_nominal":
                    # nominal pT, but including 2-point systematics
                    histogram.fill(
                        observable=observable, region=region, process=process,
                        variation=variation, weight=xsec_weight
                    )

                    if variation == "nominal":
                        # also fill weight-based variations for all nominal samples
                        for weight_name in events.systematics.fields:
                            for direction in ["up", "down"]:
                                # extract the weight variations and apply all event & region filters
                                weight_variation = events.systematics[weight_name][direction][
                                    f"weight_{weight_name}"][event_filters][region_filter]
                                # fill histograms
                                histogram.fill(
                                    observable=observable, region=region, process=process,
                                    variation=f"{weight_name}_{direction}", weight=xsec_weight * weight_variation
                                )

                        # calculate additional systematics: b-tagging variations
                        for i_var, weight_name in enumerate([f"btag_var_{i}" for i in range(4)]):
                            for i_dir, direction in enumerate(["up", "down"]):
                                # create systematic variations that depend on object properties (here: jet pT)
                                if len(observable):
                                    weight_variation = btag_weight_variation(i_var, selected_jets_region.pt)[:, i_dir]
                                else:
                                    weight_variation = 1  # no events selected
                                histogram.fill(
                                    observable=observable, region=region, process=process,
                                    variation=f"{weight_name}_{direction}", weight=xsec_weight * weight_variation
                                )

                elif variation == "nominal":
                    # pT variations for nominal samples
                    histogram.fill(
                        observable=observable, region=region, process=process,
                        variation=pt_var, weight=xsec_weight
                    )

        output = {"nevents": {events.metadata["dataset"]: len(events)}, "hist": histogram}

        return output

    def postprocess(self, accumulator):
        return accumulator


class AGCSchema(BaseSchema):
    """
    AGC `coffea` schema

    When using `coffea`, we can benefit from the schema functionality to group columns into convenient objects.
    This schema is taken from [mat-adamec/agc_coffea](https://github.com/mat-adamec/agc_coffea).
    """

    def __init__(self, base_form):
        super().__init__(base_form)
        self._form["contents"] = self._build_collections(self._form["contents"])

    def _build_collections(self, branch_forms):
        names = set([k.split('_')[0] for k in branch_forms.keys() if not (k.startswith('number'))])
        # Remove n(names) from consideration. It's safe to just remove names that start with n, as nothing else begins with n in our fields.
        # Also remove GenPart, PV and MET because they deviate from the pattern of having a 'number' field.
        names = [k for k in names if not (k.startswith('n') | k.startswith('met') |
                                          k.startswith('GenPart') | k.startswith('PV'))]
        output = {}
        for name in names:
            offsets = transforms.counts2offsets_form(branch_forms['number' + name])
            content = {k[len(name) + 1:]: branch_forms[k]
                       for k in branch_forms if (k.startswith(name + "_") & (k[len(name) + 1:] != 'e'))}
            # Add energy separately so its treated correctly by the p4 vector.
            content['energy'] = branch_forms[name + '_e']
            # Check for LorentzVector
            output[name] = zip_forms(content, name, 'PtEtaPhiELorentzVector', offsets=offsets)

        # Handle GenPart, PV, MET. Note that all the nPV_*'s should be the same. We just use one.
        # output['met'] = zip_forms({k[len('met')+1:]: branch_forms[k] for k in branch_forms if k.startswith('met_')}, 'met')
        # output['GenPart'] = zip_forms({k[len('GenPart')+1:]: branch_forms[k] for k in branch_forms if k.startswith('GenPart_')}, 'GenPart', offsets=transforms.counts2offsets_form(branch_forms['numGenPart']))
        # output['PV'] = zip_forms({k[len('PV')+1:]: branch_forms[k] for k in branch_forms if (k.startswith('PV_') & ('npvs' not in k))}, 'PV', offsets=transforms.counts2offsets_form(branch_forms['nPV_x']))
        return output

    @property
    def behavior(self):
        behavior = {}
        behavior.update(base.behavior)
        behavior.update(vector.behavior)
        return behavior


def construct_fileset():
    # ### "Fileset" construction and metadata
    #
    # Here, we gather all the required information about the files we want to
    # process: paths to the files and asociated metadata.
    fileset = utils.construct_fileset(NTUPLES_FILE, ARGS.n_files_max_per_sample,
                                      ARGS.storage_location, ARGS.merged_dataset)
    print(f"processes in fileset: {list(fileset.keys())}")
    print(f"\nexample of information in fileset:\n{{\n  'files': [{fileset['ttbar__nominal']['files'][0]}, ...],")
    print(f"  'metadata': {fileset['ttbar__nominal']['metadata']}\n}}")

    return fileset


# ### ServiceX-specific functionality: query setup
#
# Define the func_adl query to be used for the purpose of extracting columns and filtering.
def get_query(source):
    """Query for event / column selection: >=4j >=1b, ==1 lep with pT>25 GeV, return relevant columns
    """
    return source.Where(lambda e:
                        # == 1 lep
                        e.electron_pt.Where(lambda pT: pT > 25).Count() + \
                        e.muon_pt.Where(lambda pT: pT > 25).Count() == 1
                        )\
        .Where(lambda e:\
               # >= 4 jets
               e.jet_pt.Where(lambda pT: pT > 25).Count() >= 4
               )\
        .Where(lambda e:\
               # >= 1 jet with pT > 25 GeV and b-tag >= 0.5
               {"pT": e.jet_pt, "btag": e.jet_btag}.Zip().Where(lambda jet: jet.btag >= 0.5 and jet.pT > 25).Count() >= 1
               )\
        .Select(lambda e:\
                # return columns
                {
                    "electron_e": e.electron_e,
                    "electron_pt": e.electron_pt,
                    "muon_e": e.muon_e,
                    "muon_pt": e.muon_pt,
                    "jet_e": e.jet_e,
                    "jet_pt": e.jet_pt,
                    "jet_eta": e.jet_eta,
                    "jet_phi": e.jet_phi,
                    "jet_btag": e.jet_btag,
                    "numbermuon": e.numbermuon,
                    "numberelectron": e.numberelectron,
                    "numberjet": e.numberjet,
                }
                )


def _optionally_run_servicex(fileset):
    # ### Standalone ServiceX for subsequent `coffea` processing
    #
    # Using `servicex-databinder`, we can execute a query and download the output.
    # As the files are currently accessible through `rucio` only with ATLAS credentials, you need to use an ATLAS ServiceX instance to run this (for example via the UChicago coffea-casa analysis facility).

    if ARGS.pipeline == "servicex_databinder":
        from servicex_databinder import DataBinder
        t0 = time.time()

        import inspect

        # extract query from function defined previously
        query_string = inspect.getsource(get_query).split("return source.")[-1]

        sample_names = ["ttbar__nominal", "ttbar__scaledown", "ttbar__scaleup", "ttbar__ME_var", "ttbar__PS_var",
                        "single_top_s_chan__nominal", "single_top_t_chan__nominal", "single_top_tW__nominal", "wjets__nominal"]
        sample_names = ["single_top_s_chan__nominal"]  # for quick tests: small dataset with only 50 files
        sample_list = []

        for sample_name in sample_names:
            sample_list.append({"Name": sample_name, "RucioDID": f"user.ivukotic:user.ivukotic.{sample_name}",
                                "Tree": "events", "FuncADL": query_string})

        databinder_config = {
            "General": {
                "ServiceXBackendName": "uproot",
                "OutputDirectory": "outputs_databinder",
                "OutputFormat": "root",
                "IgnoreServiceXCache": ARGS.servicex_ignore_cache
            },
            "Sample": sample_list
        }

        sx_db = DataBinder(databinder_config)
        # out = sx_db.deliver(timer=True)
        parquet_paths = sx_db._sx.get_servicex_data()  # only run transform, do not download as well
        print(f"execution took {time.time() - t0:.2f} seconds")

        # point to ROOT files from databinder
        # update list of fileset files, pointing to ServiceX output for subsequent processing
        # for process in fileset.keys():
        #     if out.get(process):
        #         fileset[process]["files"] = out[process]

        # point directly to parquet files from databinder
        # update paths to point to ServiceX outputs
        for sample_name, sample_paths in zip([sample['Name'] for sample in databinder_config['Sample']], parquet_paths):
            print(f"updating paths for {sample_name} with {len(sample_paths)} parquet files (e.g. {sample_paths[0]}")
            fileset[sample_name]["files"] = sample_paths


def _update_metrics_for_coffea(metrics, fileset, exec_time):

    if ARGS.pipeline == "coffea":
        # update metrics
        dataset_source = "/data" if fileset["ttbar__nominal"]["files"][0].startswith(
            "/data") else "https://xrootd-local.unl.edu:1094"  # TODO: xcache support
        metrics.update({"walltime": exec_time, "num_workers": ARGS.ncores, "af": ARGS.analysis_facility, "dataset_source": dataset_source, "use_dask": ARGS.use_dask,
                        "systematics": SYSTEMATICS, "n_files_max_per_sample": ARGS.n_files_max_per_sample, "pipeline": ARGS.pipeline,
                        "cores_per_worker": CORES_PER_WORKER, "chunksize": CHUNKSIZE, "disable_processing": ARGS.disable_processing, "io_file_percent": ARGS.io_file_percent})

        # save metrics to disk
        if not os.path.exists("metrics"):
            os.makedirs("metrics")
        timestamp = time.strftime('%Y%m%d-%H%M%S')
        metric_file_name = f"metrics/{ARGS.analysis_facility}-{timestamp}.json"
        with open(metric_file_name, "w") as f:
            f.write(json.dumps(metrics))

        print(f"metrics saved as {metric_file_name}")
        # print(f"event rate per worker (full execution time divided by ARGS.ncores={ARGS.ncores}): {metrics['entries'] / ARGS.ncores / exec_time / 1_000:.2f} kHz")
        print(
            f"event rate per worker (pure processtime): {metrics['entries'] / metrics['processtime'] / 1_000:.2f} kHz")
        # likely buggy: https://github.com/CoffeaTeam/coffea/issues/717
        print(f"amount of data read: {metrics['bytesread']/1000**2:.2f} MB")


def analysis(fileset):
    # ### Execute the data delivery pipeline
    #
    # What happens here depends on the configuration setting for `ARGS.pipeline`:
    # - when set to `servicex_processor`, ServiceX will feed columns to `coffea` processors, which will asynchronously process them and accumulate the output histograms,
    # - when set to `coffea`, processing will happen with pure `coffea`,
    # - if `ARGS.pipeline` was set to `servicex_databinder`, the input data has already been pre-processed and will be processed further with `coffea`.

    if ARGS.pipeline == "coffea":
        if ARGS.use_dask:
            executor = processor.DaskExecutor(client=utils.get_client(ARGS.analysis_facility))
        else:
            executor = processor.FuturesExecutor(workers=ARGS.ncores)

        run = processor.Runner(executor=executor, schema=AGCSchema, savemetrics=True,
                               metadata_cache={}, chunksize=CHUNKSIZE)

        run.preprocess(fileset, treename="events")  # pre-processing

        t0 = time.monotonic()
        all_histograms, metrics = run(fileset, "events", processor_instance=TtbarAnalysis(
            ARGS.disable_processing, ARGS.io_file_percent))  # processing
        all_histograms = all_histograms["hist"]
        exec_time = time.monotonic() - t0

    elif ARGS.pipeline == "servicex_processor":
        t0 = time.monotonic()

        async def produce_all_the_histograms():
            return await utils.produce_all_histograms(fileset, get_query, TtbarAnalysis(ARGS.disable_processing, ARGS.io_file_percent), use_dask=ARGS.use_dask, ignore_cache=ARGS.servicex_ignore_cache, schema=AGCSchema)
        all_histograms = asyncio.run(produce_all_the_histograms())
        exec_time = time.monotonic() - t0

    elif ARGS.pipeline == "servicex_databinder":
        # needs a slightly different schema, not currently implemented
        raise NotImplementedError("further processing of this method is not currently implemented")
    else:
        raise NotImplementedError("Pipeline name is not recognized.")

    print(f"\nexecution took {exec_time:.2f} seconds")

    _update_metrics_for_coffea(metrics, fileset, exec_time)

    return all_histograms


def makeplots(all_histograms, fileset):
    # ### Inspecting the produced histograms
    #
    # Let's have a look at the data we obtained.
    # We built histograms in two phase space regions, for multiple physics processes and systematic variations.
    utils.set_style()

    all_histograms[120j::hist.rebin(2), "4j1b", :, "nominal"].stack(
        "process")[::-1].plot(stack=True, histtype="fill", linewidth=1, edgecolor="grey")
    plt.legend(frameon=False)
    plt.title(">= 4 jets, 1 b-tag")
    plt.xlabel("HT [GeV]")

    all_histograms[:, "4j2b", :, "nominal"].stack(
        "process")[::-1].plot(stack=True, histtype="fill", linewidth=1, edgecolor="grey")
    plt.legend(frameon=False)
    plt.title(">= 4 jets, >= 2 b-tags")
    plt.xlabel("$m_{bjj}$ [Gev]")

    # Our top reconstruction approach ($bjj$ system with largest $p_T$) has worked!
    #
    # Let's also have a look at some systematic variations:
    # - b-tagging, which we implemented as jet-kinematic dependent event weights,
    # - jet energy variations, which vary jet kinematics, resulting in acceptance effects and observable changes.
    #
    # We are making of [UHI](https://uhi.readthedocs.io/) here to re-bin.

    # b-tagging variations
    all_histograms[120j::hist.rebin(2), "4j1b", "ttbar", "nominal"].plot(label="nominal", linewidth=2)
    all_histograms[120j::hist.rebin(2), "4j1b", "ttbar", "btag_var_0_up"].plot(label="NP 1", linewidth=2)
    all_histograms[120j::hist.rebin(2), "4j1b", "ttbar", "btag_var_1_up"].plot(label="NP 2", linewidth=2)
    all_histograms[120j::hist.rebin(2), "4j1b", "ttbar", "btag_var_2_up"].plot(label="NP 3", linewidth=2)
    all_histograms[120j::hist.rebin(2), "4j1b", "ttbar", "btag_var_3_up"].plot(label="NP 4", linewidth=2)
    plt.legend(frameon=False)
    plt.xlabel("HT [GeV]")
    plt.title("b-tagging variations")

    # jet energy scale variations
    all_histograms[:, "4j2b", "ttbar", "nominal"].plot(label="nominal", linewidth=2)
    all_histograms[:, "4j2b", "ttbar", "pt_scale_up"].plot(label="scale up", linewidth=2)
    all_histograms[:, "4j2b", "ttbar", "pt_res_up"].plot(label="resolution up", linewidth=2)
    plt.legend(frameon=False)
    plt.xlabel("$m_{bjj}$ [Gev]")
    plt.title("Jet energy variations")

    # ### Save histograms to disk
    #
    # We'll save everything to disk for subsequent usage.
    # This also builds pseudo-data by combining events from the various simulation setups we have processed.
    utils.save_histograms(all_histograms, fileset, ARGS.histograms_output_file)


def main():
    fileset = construct_fileset()
    _optionally_run_servicex(fileset)
    all_histograms = analysis(fileset)
    makeplots(all_histograms, fileset)


if __name__ == "__main__":
    raise SystemExit(main())
