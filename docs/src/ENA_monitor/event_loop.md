# Event loop

The event loop is a heartbeat script that processes the next job in the loop.
Each taxon id tracked has a job stage and a priority -- 
generally later stages in the update and retrieval process have a higher priority than earlier stages
(meaning taxon ids tend to get processed to completion rather than starting lots of separate threads).
Each job will either update the stage to the next point or trigger activity on remote systems that will
update the stage via a callback.