# microfetch-pipeline
- [Overview](./README.md)

- [ENA monitor](./ENA_monitor/overview.md)
  - [Installation](./ENA_monitor/Installation.md)
  - [Workflow](./ENA_monitor/Workflow.md)
  - [Queued jobs](./ENA_monitor/queued_jobs.md)
    - [Add taxon id (manual)](./ENA_monitor/add.md)
    - [Mark data ready (callback)](./ENA_monitor/data_ready.md)
    - [Mark data collected (callback)](./ENA_monitor/data_collected.md)
  - [Event loop](./ENA_monitor/event_loop.md)
    - [Fetch accession numbers](./ENA_monitor/fetch.md)
    - [Filter accession numbers](./ENA_monitor/filter.md)
    - [Start droplet farm](./ENA_monitor/droplets.md)

- [Data collector](./Data_collector/overview.md)
  - [Collecting data](./Data_collector/collect.md)
  - [Signalling data as collected](./Data_collector/callback.md)

- [Errors and problems](./errors.md)
- [Logs](./logs.md)