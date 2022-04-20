# microfetch procedure

## Resources

- `./microfetch.csv`
    - `taxonomy_id`
      - taxon id for an organism
    - `last_checked`
      - timestamp of the last time the pipeline was run for this organism
    - `droplet_ips`
      - space-separated IP addresses for the droplets processing this taxon id's accession ids
    - `last_run_status`
      - status for the last run of the pipeline on this taxon id
      - Values:
        - '': new entries
        - 'in progress': entries where the pipeline is running
        - 'ready': entries where the data are hosted on a DigitalOcean droplet awaiting download
        - 'complete': entries where data have been downloaded
        - 'warning': the pipeline generated warnings
        - 'error': the pipeline generated errors
    - `needs_attention`
      - reason for requiring human attention
      - if not blank, nothing will be done, human inspection and repair required
    - `stage`
      - next job that will be run
    - `stage_name`
      - human-readable name of the stage
    - `priority`
      - priority in the event loop -- higher is higher priority
    - `generated_waring`
      - boolean whether the pipeline generated a warning
- `<log_dir>/<taxon_id>.log`
  - Log for the pipeline of `taxon_id`
- `./src/backend/fetch_accession_links.py`
  - Takes `taxon_id` and returns `<taxon_id>.csv`
  - TODO: modify to also take a `last_checked` value and only return results since then
- `<data_dir>/ENA_metadata/<taxon_id>.csv`
  - metadata for European Nucleotide Archive records matching `taxon_id`
- `parse_fetched_data.py`
  - Takes `<taxon_id>.csv` and returns a list of **run accession numbers**
  for valid and useful entries.
- `<data_dir>/ENA_accession_numbers/<taxon_id>.csv`
  - comma-separated value file for `taxon_id` accessions with columns:
    - `run_accession_number`
    - `excluded`
      - Whether the record is excluded by `parse_fetched_data.py`
    - `droplet_ip`
      - IP address of the droplet handling this record
- Digital Ocean droplets
  - Take a list of **run accession numbers** and save the associated genome data
  - Uses nextflow
  - Digital Ocean droplets can be managed by an API
- BDI servers need to trigger download from droplets using `rsync`
  
## Approach

### Tech backbone 

Possibilities:
  - NodeJS
  - nextflow pipeline
    - with Python sripts
  - bash

### Procedure

- Schedule two tasks:
  - Taxon id -> genome data pipeline
    - Eventually would be nice if this also ran the assembly pipeline
  - BDI server `rsync`
- Expose a script for adding taxon ids

### Taxon id -> genome data pipeline

- Find entry in `./microfetch.csv` with no `last_checked` value or with oldest `last_checked` value
- Take that entry's `taxonomy_number` and `last_checked` values and give them to `fetch_accession_links.py`
- Save the resulting .csv file and trigger `parse_fetched_data.py` for that file
- Save the resulting **run accession numbers**
- Divide the **run accession numbers** into smaller pieces
- Spin up a Digital Ocean droplet to handle each set of **run accession numbers**
  - Note droplet IPs in `./microfetch.csv` so the data can be retrieved later
  - If possible, perhaps use promises/callbacks in the API to mark when data are ready for retrieval
- Log progress throughout

### BDI server `rsync`

- Find non-empty `droplet_ips` fields in `./microfetch.csv`
- Run `rsync` download script against those IPs, removing files on success
- Destroy those droplets
- Remove IP from `droplet_ips` field
- On error:
  - note error in logs
  - do not delete IP or destroy droplets
  - mark `taxonomy_number` as in need of attention

### Add taxon ids script

- Accept any number of taxon ids as arguments
- Add novel taxon ids as entries in `./microfetch.csv`
- Summarise actions taken for the user

