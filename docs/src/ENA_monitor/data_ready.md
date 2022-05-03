# Mark data ready (callback)

When a Digital Ocean droplet has finished processing the accession numbers assigned to it,
it sends a callback to the ENA monitor via HTTP.
This HTTP request is captured by a minimal webserver and adds a mark data request to the queue.
The command is: 

```
update-droplet-status taxon_id droplet_ip new_status
```

In practice it will look something like:

```
update-droplet-status 450 8.35.22.10 ready
```

The ENA monitor simply alters the record for all accession numbers assigned to the droplet to show the given status.
When the [Data collector](../Data_collector/overview.md) queries the ENA monitor for data ready to collect,
the droplet IPs will be sent back in response.
