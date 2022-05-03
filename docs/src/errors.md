# Errors and problems

When the ENA monitor runs into problems, it logs the errors and stops working with the taxon id that 
generated the error.

A list of the taxon ids that have errored can be viewed with the verbose output of the `status` subcommand:

```shell
docker exec microfetch python /app/src/taxon_tracker.py status -v 1
```
