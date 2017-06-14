## pgha: a Postgresql multi-state Pacemaker Resource Agent

It is a modified Python translation of [PAF](https://github.com/dalibo/PAF).

It only supports Postgresql's asynchronous replication with hot standbies.

### Installation (works on CentOS/RHEL, Debian and Ubuntu)

Setting up a cluster is no simple task for the uninitiated. These will not be 
enough to get a cluster up an running from A to Z. You can take a look at [this 
tool](https://github.com/ulodciv/cluster_deployer) to help automate the setup.

These steps assume that your hosts are set up with Postgresql, corosync, pacemake, 
pcs, etc installed. Also assumed is that the Postgresql instance that will be 
replicated is installed on what will be the initial master node of the cluster 
(host1 below).

1. Setup the cluster (run these on one host only).

       pcs cluster auth host1 host2 host3 ... -u hacluster -p hacluster
       pcs cluster setup --start --name cluster1 host1 host2 host3 ...
       pcs cluster start --all

1. Use pg_basebackup to prepare each standby instances of Postgresql (here host1 
is the master, so the command should be run on host2 and host3).

       pg_basebackup -h host1 -p 5432 -D <pgdata dir> -U postgres -Xs

1. Install the resource agent (RA) on all hosts.

       cp pgha.py /usr/lib/ocf/resource.d/heartbeat/pgha
       
1. Setup the resource with correct settings (run these on one host only).

       pcs cluster cib cluster.xml
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
       pcs cluster cib-push cluster.xml

###  Some differences between PAF (pgsqlms) and pgha

- pgha uses replication slots, pgsqlms does not
- pgsqlms requires a virtual IP directing to the master instance of Postgresql,
pgha does not
- when promoting a different master:
    - pgha:
        - updates primary_conninfo's host in recovery.conf on slaves
        - restarts Postgresql on slaves
    - pgsqlms:
        - doesn't modify recovery.conf, 'host' is set to the virtual IP (VIP)
        - lets pacemaker move the VIP resource to the new master
        - the slaves eventually reconnect to the new master using the VIP: the 
        slaves are not restarted
- pgsqlms needs primary_conninfo's application_name to be set in recovery.conf, 
pgha does not
- pgha requires PG >= 9.6, pgsqlms requires PG >= 9.3
- pgha gets WAL LSN of standbies using 
GREATEST(pg_last_xlog_receive_location(), pg_last_xlog_replay_location()), pgsqlms 
gets it from pg_last_xlog_receive_location()
- pgsqlms sets a negative score to standbies that lag too much (lag > maxlag), 
pgha does not
- pgsqlms checks, during pre-promote, after a successful demotion (ie not 
after the master crashed), that the slave being promoted has the shutdown 
checkpoint, pgha does not


### TODO

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