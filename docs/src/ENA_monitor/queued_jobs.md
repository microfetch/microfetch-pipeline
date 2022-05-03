# Queued jobs

Queued commands are registered in a hidden directory (`/app/data/.queue/`) as empty files.
These commands are executed at the beginning of each program loop.
The queue is used to avoid conflicts in writing data to the central record file.