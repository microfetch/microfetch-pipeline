# Logs

The actions of the ENA monitor are recorded in logs. 
If the ENA monitor is started with the verbose flag (`-v`), the logging will be at DEBUG level,
otherwise it is INFO level.

The main log is found at `/app/data/log/main.log`, the server log at `/app/data/log/server.log`,
and the logs for individual taxon ids at `/app/data/log/<taxon_id>.log`.
