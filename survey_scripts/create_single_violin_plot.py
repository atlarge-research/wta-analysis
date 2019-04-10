#!/usr/bin/env python2.7

"""
Functions to create violin plots provided data and title.
"""

import matplotlib.pyplot as plt
import numpy as np


def create_solo_violin_plot_horizontal(data, x_label, title=None, file_name=None, log_h_axis=False):
    plt.figure(figsize=(12, 2))

    x_axis_tick_locations = []
    x_axis_tick_labels = []
    if log_h_axis:
        data = np.log10(data)
        x_axis_tick_locations = np.arange(0, int(np.ceil(max(data))))
        x_axis_tick_labels = ["$10^{0}$".format(i) for i in x_axis_tick_locations]

    plt.violinplot(data, widths=0.3, showmeans=True, showextrema=True, showmedians=True, vert=False)
    if title:
        plt.title(title, fontsize=30)

    plt.xlabel(x_label, fontsize=24)

    plt.tick_params(labelsize=20)  # Set the fontsize of the axis tick labels
    plt.yticks([])  # Disable vertical ticks
    plt.xticks(x_axis_tick_locations, x_axis_tick_labels)  # Set the x ticks manually
    plt.xlim(left=0)  # make sure the axis starts at 0
    plt.tight_layout()  # Remove white spacing around the plot
    if file_name:
        plt.savefig(file_name)
    plt.show()


def create_solo_violin_plot_vertical(data, y_label, title=None, file_name=None, log_h_axis=False):
    plt.figure(figsize=(12, 3))

    y_axis_tick_locations = []
    y_axis_tick_labels = []
    if log_h_axis:
        data = np.log10(data)
        y_axis_tick_locations = np.arange(0, int(np.ceil(max(data))))
        y_axis_tick_labels = ["$10^{0}$".format(i) for i in y_axis_tick_locations]

    plt.violinplot(data, widths=0.3, showmeans=True, showextrema=True, showmedians=True, vert=True)
    if title:
        plt.title(title, fontsize=40)

    plt.ylabel(y_label, fontsize=40)

    plt.tick_params(labelsize=30)  # Set the fontsize of the axis tick labels
    plt.yticks(y_axis_tick_locations, y_axis_tick_labels)  # Set the y ticks manually
    plt.xticks([])  # Disable horizontal ticks
    plt.ylim(ymin=0)  # make sure the axis starts at 0
    plt.tight_layout()  # Remove white spacing around the plot
    if file_name:
        plt.savefig(file_name)
    plt.show()
