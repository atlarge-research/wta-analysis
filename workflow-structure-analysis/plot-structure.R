require(data.table)
require(ggplot2)

relationship_data <- fread("SC19-structure.ssv")
relationship_to_structure <- data.table(
  relationship = c("0:0", "0:1", "0:N", "1:0", "1:1", "1:N", "N:0", "N:1", "N:N"),
  #structure = c("solo", "source", "scatter", "sink", "passthrough", "scatter", "gather", "gather", "shuffle")
  structure = factor(
    c("Standalone", "Pipeline", "Scatter", "Pipeline", "Pipeline", "Scatter", "Gather", "Gather", "Shuffle"),
    levels = c("Standalone", "Pipeline", "Gather", "Shuffle", "Scatter")
  )
)
domains <- fread("SC19-domains.ssv")

structure_data <- merge(relationship_data, relationship_to_structure, by = c("relationship"))[, .(trace, structure, count)]
structure_data <- merge(structure_data, domains, by = c("trace"))[, .(domain, structure, count)]
structure_summary <- structure_data[, .(
  count = sum(count)
), by = .(domain, structure)][order(domain, structure)]

plt <- ggplot(structure_summary) +
  geom_bar(aes(x = domain, y = count, fill = structure), position = "fill", stat = "identity") +
  theme_light(base_size=18) +
  theme(
    axis.title = element_text(size = 18),
    legend.title = element_blank(),
    legend.box.spacing = unit(0.1, 'pt'),
    legend.box.margin = margin(0, 0, 0, 0),
    legend.margin = margin(0, 0, 2, 0, 'pt'),
    legend.position = "top",
    #legend.position = c(0, 1),
    #legend.justification = c(0.265, -0.1),
    #plot.margin = margin(25, 2, 1, 1, "pt"),
    plot.margin = margin(2, 2, 1, 1, "pt"),
    legend.direction = "horizontal"
  ) +
  guides(fill = guide_legend(reverse = TRUE)) +
  scale_fill_grey() +
  xlab("Domain") +
  ylab("Fraction of tasks") +
  coord_flip()

ggsave("workload_structures.pdf", plt, width = 16, height = 6, units = c("cm"))

total_count_per_domain <- structure_summary[, .(total.count = sum(count)), by = .(domain)]
structure_fraction_per_domain <- merge(structure_summary, total_count_per_domain, by = c("domain"))[order(domain, structure), .(
  domain,
  structure,
  fraction = count / total.count
)]
