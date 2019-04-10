
import os
import gc
import json
import numpy as np
from plotnine import *
import pandas as pd
from scipy.optimize import curve_fit, least_squares
from scipy.stats import linregress
from scipy import stats
from palettable.colorbrewer.qualitative import Paired_7

from dist_functions import *


feature_to_xlab_map = {
    "interarrival": "Interarrival Time of Tasks (ms)",
    "lifetime": "Lifetime of Tasks (ms)"
}

feature_to_ylab_map = {
    "interarrival": "Fraction of tasks",
    "lifetime": "Fraction of tasks"
}

sf_functions = {
    "weibull": weibull_sf,
    "gen_pareto": gen_pareto_sf,
    "expon": expon_sf,
    "gamma": gamma_sf,
    "lognormal": lognormal_sf,
    "levy": levy_sf
}

pdf_functions = {
    "weibull": weibull_pdf,
    "gen_pareto": gen_pareto_pdf,
    "expon": expon_pdf,
    "gamma": gamma_pdf,
    "lognormal": lognormal_pdf,
    "levy": levy_pdf
}

cdf_functions = {
    "weibull": weibull_cdf,
    "gen_pareto": gen_pareto_cdf,
    "expon": expon_cdf,
    "gamma": gamma_cdf,
    "lognormal": lognormal_cdf,
    "levy": levy_cdf
}


def do_regression(available_data, target_directory, source_directory):
    for data_type, features in available_data.items():
        for feature_name, feature_values in features.items():
            # if feature_name not in ["interarrivalTimeByCluster", "interarrivalTimeOfFiles",
            #                         "pathCountOverall", "sizeCountOverall"]:
            if feature_name not in ["interarrival", "lifetime"]:
                # lifetimeOfFiles and interaccessTimeOverall left out
                continue
            # if feature_name not in ["pathCountOverall"]:
            #     continue
            print(feature_name)
            gc.collect()

            # dataset_jan = pd.read_csv(os.path.join(source_directory, feature_values["january"]), header=0)
            dataset_may = pd.read_csv(os.path.join(source_directory, feature_values["filename"]), header=0)

            # dataset_jan.sort_values(by=dataset_jan.columns[0], inplace=True)
            dataset_may.sort_values(by=dataset_may.columns[0], inplace=True)

            # dataset_jan.reset_index(drop=True, inplace=True)
            dataset_may.reset_index(drop=True, inplace=True)

            gc.collect()

            canonical_name = feature_name

            storage_loc = os.path.join(target_directory,
                                       data_type + "_regressed",
                                       canonical_name)
            os.makedirs(os.path.join(target_directory, data_type + "_regressed"), exist_ok=True)

            feature_column_name = feature_name
            
            bins = np.logspace(start=0, stop=np.log10(dataset_may[feature_column_name].max()), num=1000, endpoint=False, base=10)
            generated_bins, new_hist = rebin(dataset_may[feature_column_name], dataset_may["count"], bins)
            new_dataset = pd.DataFrame({
                feature_column_name: generated_bins,
                "count": new_hist
            })
            
            if new_dataset.shape[0] == 0:
                continue

            try:
                new_dataset = normalize_count(new_dataset, feature_column_name)
            except Exception as e:
                print(new_dataset)
                return
            
            if new_dataset.shape[0] == 0:
                continue
            
            setup_plots(new_dataset, sf_functions, feature_name, storage_loc, "survival")
            gc.collect()
            setup_plots(new_dataset, pdf_functions, feature_name, storage_loc, "pdf")
            gc.collect()

            setup_plots(new_dataset, sf_functions, feature_name, storage_loc, "survival", "weighted")
            gc.collect()
            setup_plots(new_dataset, pdf_functions, feature_name, storage_loc, "pdf", "weighted")
            gc.collect()


def rebin(feature_column, value_column, new_bins):
    new_hist = []
    generated_bins = []
    prev_binvalue = 0
    for (index,), binvalue in np.ndenumerate(new_bins):
        sum_of_counts = 0
        if index == 0:
            sum_of_counts = value_column[feature_column <= binvalue].sum()
        elif index == len(new_bins) - 1:
            sum_of_counts = value_column[feature_column > prev_binvalue].sum()
        else:
            sum_of_counts = value_column[(feature_column <= binvalue) & (feature_column > prev_binvalue)].sum()

        if sum_of_counts > 0:
            new_hist.append(sum_of_counts)
            generated_bins.append(binvalue)
        prev_binvalue = binvalue

    return generated_bins, new_hist


def normalize_count(dataset, feature_column_name):
    dataset["pdf"] = dataset["count"] / dataset["count"].sum()
    dataset["cdf"] = dataset["pdf"].cumsum()
    dataset["survival"] = 1 - dataset["cdf"]

    dataset.loc[1:, "weight"] = 1 / (dataset.loc[1:, feature_column_name] - dataset.loc[0, feature_column_name])

    dataset = dataset[~(dataset["survival"] < 0)].copy()
    dataset.drop(dataset.index[0], inplace=True) # Removing the first row as log(1) = 0 causes problems.
    return dataset.reset_index(drop=True)


def formatOneLabel(x):
    if x == 0:
        return "0"
    else:
        exponentNum = str(np.int32(np.floor(np.log10(x))))
        return "$10^{"+ exponentNum +"}$"


def formatYaxisLabels(xl):
    return list(map(formatOneLabel, xl))


def getLegendPosition(regression_type):
    if regression_type == "pdf":
        return (0.6, 0.6)
    elif regression_type == "sf":
        return (0.3, 0.3)


def measure_difference(original, computed, fileprefix, regression_type, weight_nature):
    dataset_col_to_measure = original.loc[computed.index, "pdf"]
    # log_dataset_col = np.log(dataset_col_to_measure)

    ks_dist = np.max(np.abs(original.loc[computed.index, "cdf"] - computed["cdf"]))
    chi2_dist, chi2_p = stats.chisquare(computed["pdf"], dataset_col_to_measure)

    # print(np.sum(np.logical_not(np.isfinite(np.log(computed["pdf"])))))
    # print(computed["pdf"][np.logical_not(np.isfinite(np.log(computed["pdf"])))])

    # kl_divergence = np.dot(dataset_col_to_measure, log_dataset_col - np.log(computed["pdf"]))

    return {
        "ks_dist": ks_dist,
        "chi2_dist": chi2_dist,
        "chi2_p": chi2_p
    }


def setup_plots(dataset, func_dict, feature_name, fileprefix, regression_type, weight_nature="unweighted"):
    feature_column_name = dataset.columns[0]

    xlabel = feature_to_xlab_map[feature_name]
    ylabel = feature_to_ylab_map[feature_name]

    regressed_data_dfs = {}
    diffs_quantified = {}

    for dist_name, func in func_dict.items():
        try:
            if weight_nature == "weighted":
                popt, pcov = curve_fit(func, dataset[feature_column_name], dataset[regression_type], maxfev=1000,
                                       sigma=dataset["weight"], absolute_sigma=True)
            else:
                popt, pcov = curve_fit(func, dataset[feature_column_name], dataset[regression_type], maxfev=1000)

            df = pd.DataFrame({
                "variates": dataset[feature_column_name],
                "pdf": pdf_functions[dist_name](dataset[feature_column_name], *popt),
                "cdf": cdf_functions[dist_name](dataset[feature_column_name], *popt),
                "survival": sf_functions[dist_name](dataset[feature_column_name], *popt),
                "dist_name": dist_name
            })

            # Filter NaN values
            df = df[np.isfinite(df["pdf"]) & (df["pdf"] > 0)]
            df = df[np.isfinite(df["survival"]) & (df["survival"] > 0)]

            regressed_data_dfs[dist_name] = df

            diffs = measure_difference(dataset, df, fileprefix, regression_type, weight_nature)
            diffs_quantified[dist_name] = diffs
        except RuntimeError:
            print("Unable to fit: {} to {} with {}", regression_type, dist_name, weight_nature)

    with open("{}_{}_{}.json".format(fileprefix, regression_type, weight_nature), "w") as f:
        json.dump(diffs_quantified, f, indent=2)

    dataset["dist_name"] = "original"

    # print(dataset[[feature_column_name, regression_type]])
    # print(dataset[regression_type].min())
    # print(dataset.shape)

    plt_layers = ggplot(dataset) + \
                 theme_light(base_size=16) + \
                 theme(legend_title=element_text(size=0, alpha=0),
                       legend_box_spacing=0.1,
                       legend_box_margin=0,
                       legend_margin=0) + \
                 geom_point(aes(x=feature_column_name, y=regression_type, color="dist_name")) + \
                 scale_x_log10(labels=formatYaxisLabels) + \
                 scale_y_log10(limits=(dataset[regression_type].min(), 1), labels=formatYaxisLabels) + \
                 xlab(xlabel) +\
                 ylab(ylabel)

    for dist_name, df in regressed_data_dfs.items():
        plt_layers = plt_layers + geom_line(aes(x="variates", y=regression_type, color="dist_name"), data=df)

    # fig = plt_layers.draw()
    # axs = fig.get_axes()
    # ax1 = axs[0]
    # ax1.set_yscale('log')
    # ax1.set_ybound(lower=dataset[regression_type].min(), upper=1)
    #
    # fig.savefig("{}_{}_{}.png".format(fileprefix, regression_type, weight_nature), bbox_inches='tight', dpi=300)

    plt_layers.save("{}_{}_{}.png".format(fileprefix, regression_type, weight_nature), dpi=300)
