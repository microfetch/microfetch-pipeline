## microfetch-pipeline

The microfetch-pipeline tracks European Nucleotide Archive entries by taxon identifiers and
retrieves and filters the associated genomic data.
These data are processed on a Digital Ocean droplet and the result fetched by servers at the 
Big Data Institute.

The pipeline has two separate components, the [ENA monitor](./ENA_monitor/overview.md) and the 
[Data collector](./Data_collector/overview.md).
