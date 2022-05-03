# Mark data collected (callback)

When the [Data collector](../Data_collector/overview.md) has collected data from a droplet it
executes a small shell script callback:

```shell
python /app/src/taxon_tracker.py mark-collected -t taxon_id -d droplet_ip
```

The ENA monitor will then mark the droplet data as collected and destroy the droplet.
If all droplets have had their data collected, the process is complete and 
the ENA monitor will mark the pipeline to start again from the beginning.
