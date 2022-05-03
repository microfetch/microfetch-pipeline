# Create droplet farm

Once the list of accession numbers has been filtered to those that are good candidates for retrieval,
the actual data are pulled down from the ENA using Digital Ocean droplets. 
Filtered accession numbers are divided up among the available droplets, 
and those droplets are initialized and left to run.

The file `/app/data/ENA_accession_filtered/<taxon_id>.csv` is updated to include droplet metadata:

| accession	      | droplet_group	 | droplet_ip	 | status     |
|-----------------|----------------|-------------|------------|
| SAMEA4076733	   | 0	             | 127.0.0.1	  | processing |
| SAMEA104200669	 | 1	             | 127.0.0.1	  | processing |
| SAMEA4598506	   | 0	             | 127.0.0.1	  | processing |
| SAMEA4598507	   | 1	             | 127.0.0.1	  | processing |

When the droplets complete, they issue a [data ready callback](data_ready.md).

This step will only be run when droplets are available.
If another taxon id is using the droplets, they will not be created until the data have been collected
and the existing droplets destroyed.
