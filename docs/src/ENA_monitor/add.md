# Add taxon ids to the tracker

Taxon ids can be added using the `add` subcommand.
Assuming you have the docker container for the app running as `microfetch`, 
add taxon ids with the command:

```shell
docker exec microfetch python /app/src/taxon_tracker.py add 450 470 570 750
```

The above command will add the taxon ids 450, 470, 570, and 750 to tracking.
If you are monitoring the output or logs for the main process,
you'll see those being added before the next [event loop](event_loop.md) step.
