# Fetch accession numbers

The ENA stores records by 'accession numbers'. 
These numbers are accessed by taxon identifier. 
The first step in the ENA monitor pipeline is to use a taxon id to fetch accession numbers.

The fetching is done via a backend script that uses HTTP requests to the ENA API.
When the API is successfully contacted, it returns a CSV file that is stored as
`/app/data/ENA_accession_metadata/<taxon_id>.csv`.

This file has the form:

| experiment_accession	 | accession	    | sample_accession	 | secondary_sample_accession |
|-----------------------|---------------|-------------------|----------------------------|
| ERX1851229	           | SAMEA4362422	 | SAMEA4362422	     | ERS1273871                 |
| SRX1972698	           | SAMN02256443	 | SAMN02256443	     | SRS1581087                 |
| SRX337460	            | SAMN02298051	 | SAMN02298051	     | SRS471889                  |
| SRX346404	            | SAMN02344103	 | SAMN02344103	     | SRS477544                  |
| SRX346419	            | SAMN02344107	 | SAMN02344107	     | SRS477553                  |

Its contents will be filtered for relevance and quality in the next step.
