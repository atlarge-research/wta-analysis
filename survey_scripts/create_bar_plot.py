#!/usr/bin/env python2.7

import matplotlib.pyplot as plt
import numpy as np


def create_horizontal_bar_plot(labels, sizes, horizontal_axis_label, file_name=None, show=False):
    fig, ax = plt.subplots(figsize=(6, 2))

    y_pos = np.arange(len(labels))

    grey_out = ["uncategorized"]
    matching_indices = [index for index, label in enumerate(labels) if label.lower() in grey_out]

    bars = ax.barh(y_pos, sizes, height=0.6, align='center', color='black')

    for index in matching_indices:
        bars[index].set_color('grey')

    ax.set_yticks(y_pos)
    ax.set_yticklabels(labels, fontsize=14)
    ax.invert_yaxis()  # labels read top-to-bottom
    ax.set_xlabel(horizontal_axis_label, fontsize=14)

    plt.tight_layout()

    if file_name:
        plt.savefig(file_name)

    if show:
        plt.show()
