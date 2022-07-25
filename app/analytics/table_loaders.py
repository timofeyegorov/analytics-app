import os
import pickle as pkl
from config import RESULTS_FOLDER


def get_channels_summary():
    with open(os.path.join(RESULTS_FOLDER, "channels_summary.pkl"), "rb") as f:
        data = pkl.load(f)
    return data


def get_channels_detailed():
    with open(os.path.join(RESULTS_FOLDER, "channels_detailed.pkl"), "rb") as f:
        data = pkl.load(f)
    return data


def get_payments_accumulation():
    with open(os.path.join(RESULTS_FOLDER, "payments_accumulation.pkl"), "rb") as f:
        data = pkl.load(f)
    return data


def get_marginality():
    with open(os.path.join(RESULTS_FOLDER, "marginality.pkl"), "rb") as f:
        data = pkl.load(f)
    return data


def get_audience_type():
    with open(os.path.join(RESULTS_FOLDER, "audience_type.pkl"), "rb") as f:
        data = pkl.load(f)
    return data


def get_audience_type_percent():
    with open(os.path.join(RESULTS_FOLDER, "audience_type_percent.pkl"), "rb") as f:
        data = pkl.load(f)
    return data


def get_segments():
    with open(os.path.join(RESULTS_FOLDER, "segments.pkl"), "rb") as f:
        data = pkl.load(f)
    return data


def get_clusters():
    with open(os.path.join(RESULTS_FOLDER, "clusters.pkl"), "rb") as f:
        data = pkl.load(f)
    return data


def get_turnover():
    with open(os.path.join(RESULTS_FOLDER, "turnover.pkl"), "rb") as f:
        data = pkl.load(f)
    return data


def get_landings():
    with open(os.path.join(RESULTS_FOLDER, "landings.pkl"), "rb") as f:
        data = pkl.load(f)
    return data


def get_traffic_sources():
    with open(os.path.join(RESULTS_FOLDER, "traffic_sources.pkl"), "rb") as f:
        data = pkl.load(f)
    return data


def get_segments_stats():
    with open(os.path.join(RESULTS_FOLDER, "segments_stats.pkl"), "rb") as f:
        data = pkl.load(f)
    return data


def get_leads_ta_stats():
    with open(os.path.join(RESULTS_FOLDER, "leads_ta_stats.pkl"), "rb") as f:
        data = pkl.load(f)
    return data


def get_channels():
    with open(os.path.join(RESULTS_FOLDER, "leads.pkl"), "rb") as f:
        data = pkl.load(f)
    return data


if __name__ == "__main__":
    df = get_payments_accumulation()
    df2 = get_channels_summary()
    print(type(df), type(df2))
