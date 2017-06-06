# pgha

A modified Python translation of [PAF](https://github.com/dalibo/PAF), a 
multi-state Pacemaker Resource Agent (RA) for Postgresql.

### Usage

1. Install the resource agent (RA):

       cp pgha.py /usr/lib/ocf/resource.d/heartbeat/pgha
       
2. Setup the resource with correct settings (e.g. pgbindir, pgdata):

       pcs -f cluster.xml resource create pg \
       ocf:heartbeat:pgha \
       pgbindir=/usr/pgsql-9.6/bin \
       pgdata=/var/lib/pgsql/9.6/data \
       pgconf=/var/lib/pgsql/9.6/data/postgresql.conf \
       pgport=5432 \
       pghost=/tmp \
       op start timeout=60s \
       op stop timeout=60s \
       op promote timeout=120s \
       op demote timeout=120s \
       op monitor interval=10s timeout=10s role="Master" \
       op monitor interval=11s timeout=10s role="Slave" \
       op notify timeout=60s

###  Some differences between PAF (pgsqlms) and pgha

- pgha uses replication slots, pgsqlms does not
- pgsqlms requires a virtual IP directing to the master instance of Postgresql,
pgha does not
- when promoting a master:
    - pgha:
        - updates 'host' in 'primary_conninfo' in recovery.conf on slaves
        - restarts Postgresql on slaves
    - pgsqlms:
        - doesn't modify recovery.conf, 'host' is set to the virtual IP (VIP)
        - lets pacemaker move the VIP resource to the new master
        - the slaves eventually reconnect to the new master using the VIP: the 
        slaves are not restarted
- pgsqlms needs application_name to be set in primary_conninfo in recovery.conf, 
pgha does not
- pgha requires PG >= 9.6, pgsqlms requires PG >= 9.3
- pgha gets WAL LSN from pg_last_xlog_replay_location, pgsqlms gets it from 
pg_last_xlog_receive_location
- pgsqlms sets a negative score to standbies that lag too much (lag > maxlag), 
pgha does not
- pgsqlms checks, during pre-promote, that the slave being promoted has the 
shutdown checkpoint, pgha does not


### TODO

- Untangle code by eliminating all calls to get_ocf_state.

- Explore how RA could try to clean up a crashed master instance by starting 
it and shutting it down. This would be sufficient only if the promoted slave was
all WAL caught up with the master that crashed.

- RA should report a slave that doesn't replicate because its
timeline diverged as failed. Perhaps this should bring the whole resource down. 
Technically, this should not occur, however, because the RA is suppose to prevent this. 
	
        LOG:  fetching timeline history file for timeline 3 from primary server
        FATAL:  could not start WAL streaming: ERROR:  requested starting point 0/9000000 
        on timeline 2 is not in this server's history
        DETAIL:  This server's history forked from timeline 2 at 0/80001A8.

- Explore using 
[pg_rewind](https://wiki.postgresql.org/wiki/What's_new_in_PostgreSQL_9.5#pg_rewind) 
to optionally rewind a master that crashed.