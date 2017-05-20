# pgha

A modified Python translation of [PAF](https://github.com/dalibo/PAF), a 
multi-state Pacemaker Resource Agent (RA) for Postgresql.

##  Some differences between PAF (pgsqlms) and pgha

- pgha uses replication slots, pgsqlms does not
- pgsqlms needs application_name to be set in primary_conninfo in recovery.conf, pgha does not
- pgha requires PG >= 9.6, pgsqlms requires PG >= 9.3
- pgha gets WAL LSN from pg_last_xlog_replay_location, pgsqlms gets it from pg_last_xlog_receive_location
- pgsqlms sets a negative score to standbies that lag too much (lag > maxlag), pgha does not
- pgsqlms checks, during pre-promote, that the slave being promoted has the shutdown checkpoint, pgha does not.

## TODO

- Untangle code by eliminating all calls to get_ocf_state.

- Explore how RA could try to clean up a crashed master instance by starting 
it and shutting it down. This would be sufficient only if the promoted slave was
all WAL caught up with the master that crashed.

- RA should report a slave that doesn't replicate because its
timeline diverged as down. Perhaps this should bring the whole resource down. 
Technically, this should not occur, however, because the RA is suppose to prevent this. 
	
        LOG:  fetching timeline history file for timeline 3 from primary server
        FATAL:  could not start WAL streaming: ERROR:  requested starting point 0/9000000 
        on timeline 2 is not in this server's history
        DETAIL:  This server's history forked from timeline 2 at 0/80001A8.

- Explore using 
[pg_rewind](https://wiki.postgresql.org/wiki/What's_new_in_PostgreSQL_9.5#pg_rewind) 
to optionally rewind a master that crashed.