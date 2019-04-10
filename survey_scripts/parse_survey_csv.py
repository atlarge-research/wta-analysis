import datetime
import re

import numpy as np
import pandas as pd

from survey_scripts.create_bar_plot import create_horizontal_bar_plot
from survey_scripts.create_single_violin_plot import create_solo_violin_plot_horizontal

path_to_csv = "../Literature_survey-usage_of_WFs - 2009-2018_2019-03-26.csv"


def generate_barplots(date, domains, fields):
    domain_count = dict()
    field_count = dict()

    for domain_string in domains:
        for domain in domain_string.split(","):
            domain = domain.strip()
            if len(domain) == 0: continue
            if domain.lower() == 'unknown':
                domain = 'uncategorized'
            cleaned_domain = domain.lower().strip()
            domain_count[cleaned_domain] = domain_count.get(cleaned_domain, 0) + 1

    for field_string in fields:
        for field in field_string.split(","):
            field = field.strip()
            if len(field) == 0: continue
            if field.lower() == 'unknown':
                field = 'uncategorized'
            cleaned_field = field.lower()
            field_count[cleaned_field] = field_count.get(cleaned_field, 0) + 1

    print("Num domains: ", len(domain_count.keys()))
    print("Num fields: ", len(field_count.keys()))

    s = [(k, domain_count[k]) for k in sorted(domain_count, key=domain_count.get, reverse=True)]
    print(s)
    domain_labels = []
    domain_values = []
    for k, v in s[:5]:  # Print the top 5
        domain_labels.append(k.title())
        domain_values.append(v)

    field_labels = []
    field_values = []
    s = [(k, field_count[k]) for k in sorted(field_count, key=field_count.get, reverse=True)]
    print(s)
    for k, v in s[:5]:  # Print the top 5
        field_labels.append(k.title())
        field_values.append(v)

    create_horizontal_bar_plot(domain_labels, domain_values, "Count", file_name=
    "{0:%Y-%m-%d_%H-%M-%S}_domain_count.pdf".format(
        date))
    create_horizontal_bar_plot(field_labels, field_values, "Count", file_name=
    "{0:%Y-%m-%d_%H-%M-%S}_field_count.pdf".format(
        date))


with open(path_to_csv) as csv_file:
    df = pd.read_csv(csv_file).fillna("?")

    venue_dict = dict()
    num_types_list = []
    largest_wf_list = []
    smallest_wf_list = []
    domain_strings = []
    field_strings = []
    num_traces_used = []

    total_papers_using_traces = 0
    total_papers_using_realistic_traces = 0
    total_papers_using_open_source_traces = 0

    for _, row in df.iterrows():
        venue = row[0]
        uses_wf_traces = "y" in row[4]
        real_world_traces = "y" in row[5]
        num_types = int(row[6]) if re.match("^[0-9]+.*$", str(row[6])) else 0
        data_open_source = "y" in row[7]
        num_wf_traces = int(row[8]) if re.match("^[0-9]+.*$", row[8]) else 0
        largest_wf = int(row[9].strip()) if "?" not in row[9] else -1
        smallest_wf = int(row[10].strip()) if "?" not in row[10] else -1

        domains = row[12]
        fields = row[13]

        count = venue_dict.get(venue, [0, 0, 0])  # number of hits, number using traces, number of open source traces

        if uses_wf_traces:
            count[0] += 1
            total_papers_using_traces += 1

        if real_world_traces:
            count[1] += 1
            total_papers_using_realistic_traces += 1

        if data_open_source:
            count[2] += 1
            total_papers_using_open_source_traces += 1

        venue_dict[venue] = count

        if uses_wf_traces:
            domain_strings.append(domains)
            field_strings.append(fields)

        if num_types > 0:
            num_types_list.append(num_types)

        if largest_wf > 0:
            largest_wf_list.append(largest_wf)

        if smallest_wf > 0:
            smallest_wf_list.append(smallest_wf)

        if num_wf_traces > 0:
            num_traces_used.append(num_wf_traces)

    date = datetime.datetime.now()
    generate_barplots(date, domain_strings, field_strings)
    # Create violin plots showing the distribution of number of domains, smallest and largest WF size.
    # create_solo_violin_plot_horizontal(num_types_list, "Number of workflow types",
    #                                    file_name="{0:%Y-%m-%d_%H-%M-%S}_workflow_type_distribution_horizontal.pdf".format(
    #                                        date))
    # create_solo_violin_plot_horizontal(largest_wf_list, "Number of tasks",
    #                                    file_name="{0:%Y-%m-%d_%H-%M-%S}_largest_wf_size_distribution_horizontal.pdf".format(
    #                                        date), log_h_axis=True)
    # create_solo_violin_plot_horizontal(smallest_wf_list, "Number of tasks",
    #                                    file_name="{0:%Y-%m-%d_%H-%M-%S}_smallest_wf_size_distribution_horizontal.pdf".format(
    #                                        date), log_h_axis=True)
    # create_solo_violin_plot_horizontal(num_traces_used, "Number of workflows",
    #                                    file_name="{0:%Y-%m-%d_%H-%M-%S}_wf_count_distribution_horizontal.pdf".format(
    #                                        date), log_h_axis=True)

    print("Avg num traces used", np.average(num_traces_used))
    print("median num traces used", np.median(num_traces_used))
    print("max num traces used", np.max(num_traces_used))

    print("Avg smallest WF", np.average(smallest_wf_list))
    print("median smallest WF", np.median(smallest_wf_list))
    print("max smallest WF", np.max(smallest_wf_list))

    print("Avg largest WF", np.average(largest_wf_list))
    print("median largest WF", np.median(largest_wf_list))
    print("max largest WF", np.max(largest_wf_list))

    # Venues below the threshold can be combined into "others"
    combine_below_threshold = True
    threshold = 5
    if combine_below_threshold:
        new_venue_dict = dict()
        count_other = [0, 0, 0]
        combined_venues = ""
        for venue, values in sorted(venue_dict.items(), key=lambda pair: pair[1], reverse=True):
            if values[0] == 0: continue

            if values[0] <= threshold:
                combined_venues += venue + ", "
                count_other[0] += values[0]
                count_other[1] += values[1]
                count_other[2] += values[2]
            else:
                new_venue_dict[venue] = values

        new_venue_dict["other"] = count_other
        venue_dict = new_venue_dict
        print(combined_venues)

    table_header = " & Total"
    table_line_amount_of_papers_using_traces = "Articles using traces & {0} (100\%)".format(
        total_papers_using_traces)
    table_line_amount_of_papers_using_realistic_traces = "articles using \\emph{{realistic}} traces & {0} ({1:.0f}\%)".format(
        total_papers_using_realistic_traces, float(total_papers_using_realistic_traces)/total_papers_using_traces * 100)
    table_line_amount_of_papers_using_open_source_traces = "Articles using traces that are both {{\it realistic}} and {{\it open-access}} & {0} ({1:.0f}\%)".format(
        total_papers_using_open_source_traces, float(total_papers_using_open_source_traces)/total_papers_using_traces * 100)

    for venue, values in sorted(venue_dict.items(), key=lambda pair: pair[1][0] if pair[0] != "other" else -1, reverse=True):
        if values[0] == 0: continue
        table_header += " & " + venue
        table_line_amount_of_papers_using_traces += " & " + str(values[0])

        using_realistic_trace_part = " & " + str(values[1])
        using_open_source_traces_part = " & " + str(values[2])
        if values[0] > 10:
            table_line_amount_of_papers_using_traces += (" (100\%)")
            using_realistic_trace_part += " ({0:.0f}\%)".format(float(values[1]) / float(values[0]) * 100)
            using_open_source_traces_part += " ({0:.0f}\%)".format(float(values[2]) / float(values[0]) * 100)

        table_line_amount_of_papers_using_realistic_traces += using_realistic_trace_part
        table_line_amount_of_papers_using_open_source_traces += using_open_source_traces_part

    table_line_amount_of_papers_using_traces += "\\\\"
    table_line_amount_of_papers_using_realistic_traces += "\\\\"
    table_line_amount_of_papers_using_open_source_traces += "\\\\"
    table_header += "\\\\ \\midrule"

    print("""
    \\begin{{table*}}[]
    \\setlength{{\\tabcolsep}}{{4pt}}
    \\centering
    \\caption{{Workflow trace usage in venues having at least one paper returned in the initial query.\\\\Percentages only shown for venues with $>${0} hits.}} \\label{{tbl:workflow-trace-usage-venues}}
    \\begin{{tabular}}{{l{1}}} \\toprule
    {2}
    {3}
    {4}
    {5} \\bottomrule
    \end{{tabular}}
    \end{{table*}}
    """.format(threshold, "r" * (len(venue_dict.keys()) + 1), table_header, table_line_amount_of_papers_using_traces,
               table_line_amount_of_papers_using_realistic_traces,
               table_line_amount_of_papers_using_open_source_traces))
