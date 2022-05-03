# ENA monitor

The ENA monitor is the main component. 
This program can be given taxon ids to track.
Tracked taxon ids will be serially processed, meaning they are looked up on the ENA database,
the records retrieved and filtered, and the resulting records sent to a Digital Ocean droplet farm for processing.
When processing is complete, the Digital Ocean droplet informs the ENA monitor, which in turn informs the
[Data collector](../Data_collector/overview.md). 

The overview of the ENA monitor can be seen with the `status` subcommand:
```shell
docker exec microfetch python /app/src/taxon_tracker.py status
```

The status presents a summary of the central CSV file. 
For more detailed information you can inspect that file directly (`/app/data/microfetch.csv`).
It looks like:

| taxon_id	 | last_checked	 | last_run_status	 | needs_attention	 | stage | stage_name            | priority	 | generated_warning	 | checkpoint_time            |
|-----------|---------------|------------------|------------------|-------|-----------------------|-----------|--------------------|----------------------------|
| 570	      | <NA>	         | in progress		    |                  | 3	    | filter accession CSV	 | 3		       |                    | 2022-05-03 09:09:51.093475 |
| 750	      | <NA>	         | error		          |                  | 4	    | create droplet farm	  | 0	        | 	                  | 2022-04-22 19:18:43.476658 |
| 755	      | <NA>	         | error		          |                  | 4	    | create droplet farm	  | 0	        | 	                  | 2022-05-03 08:53:01.363596 |
