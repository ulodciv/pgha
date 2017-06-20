#!/usr/bin/python2.7
from __future__ import division, print_function, unicode_literals
import json
import os
import pipes
import pwd
import re
import sys
import syslog
import tempfile
from datetime import datetime
from distutils.version import LooseVersion
from functools import partial
from subprocess import call, check_output, CalledProcessError, STDOUT
from tempfile import gettempdir
from time import sleep

VERSION = "1.0"
PROGRAM = "pgha"
MIN_PG_VER = LooseVersion('9.6')
OCF_SUCCESS = 0
OCF_ERR_GENERIC = 1
OCF_ERR_ARGS = 2
OCF_ERR_UNIMPLEMENTED = 3
OCF_ERR_PERM = 4
OCF_ERR_INSTALLED = 5
OCF_ERR_CONFIGURED = 6
OCF_NOT_RUNNING = 7
OCF_RUNNING_MASTER = 8
OCF_FAILED_MASTER = 9
OCF_EXIT_CODES = {
    OCF_SUCCESS: "SUCCESS",
    OCF_ERR_GENERIC: "ERR_GENERIC",
    OCF_ERR_ARGS: "ERR_ARGS",
    OCF_ERR_UNIMPLEMENTED: "ERR_UNIMPLEMENTED",
    OCF_ERR_PERM: "ERR_PERM",
    OCF_ERR_INSTALLED: "ERR_INSTALLED",
    OCF_ERR_CONFIGURED: "ERR_CONFIGURED",
    OCF_NOT_RUNNING: "NOT_RUNNING",
    OCF_RUNNING_MASTER: "RUNNING_MASTER",
    OCF_FAILED_MASTER: "FAILED_MASTER"}
RE_TPL_SLOT = r"^\s*primary_slot_name\s*=\s*'?(.*?)'?\s*$"
RE_TPL_TIMELINE = r"^\s*recovery_target_timeline\s*=\s*'?latest'?\s*$"
RE_TPL_STANDBY_MODE = r"^\s*standby_mode\s*=\s*'?on'?\s*$"
RE_PG_CLUSTER_STATE = r"^Database cluster state:\s+(.*?)\s*$"
pguser_default = "postgres"
pgbindir_default = "/usr/bin"
pgdata_default = "/var/lib/pgsql/data"
pghost_default = "/var/run/postgresql"
pgport_default = "5432"
OCF_META_DATA = """\
<?xml version="1.0"?>
<!DOCTYPE resource-agent SYSTEM "ra-api-1.dtd">
<resource-agent name="pgsqlha">
<version>1.0</version>
<longdesc lang="en">Multi-state PostgreSQL resource agent. It manages 
PostgreSQL instances using streaming replication.</longdesc>
<shortdesc lang="en">Manages replicated PostgreSQL instances</shortdesc>
<parameters>
    <parameter name="pguser" unique="0" required="0">
    <longdesc lang="en">PostgreSQL server user</longdesc>
    <shortdesc lang="en">PostgreSQL server user</shortdesc>
    <content type="string" default="{pguser_default}" />
</parameter>
<parameter name="pgbindir" unique="0" required="0">
    <longdesc lang="en">Directory with PostgreSQL binaries. This RA uses 
    psql, pg_isready, pg_controldata and pg_ctl.</longdesc>
    <shortdesc lang="en">Path to PostgreSQL binaries</shortdesc>
    <content type="string" default="{pgbindir_default}" />
</parameter>
<parameter name="pgdata" unique="1" required="0">
    <longdesc lang="en">Data directory</longdesc>
    <shortdesc lang="en">pgdata</shortdesc>
    <content type="string" default="{pgdata_default}" />
</parameter>
<parameter name="pghost" unique="0" required="0">
    <longdesc lang="en">Host IP address or unix socket folder.</longdesc>
    <shortdesc lang="en">pghost</shortdesc>
    <content type="string" default="{pghost_default}" />
</parameter>
<parameter name="pgport" unique="0" required="0">
    <longdesc lang="en">PostgreSQL port.</longdesc>
    <shortdesc lang="en">pgport</shortdesc>
    <content type="integer" default="{pgport_default}" />
</parameter>
<parameter name="pgconf" unique="0" required="0">
    <longdesc lang="en">
    Additionnal arguments for 'pg_ctl start' for the -o parameter. 
    Can be use when postgresql.conf file is not in PGDATA, eg.:
    "-c config_file=/etc/postgresql/9.6/main/postgresql.conf".</longdesc>
    <shortdesc lang="en">Additionnal arguments for 'pg_ctl start'.</shortdesc>
    <content type="string" default="{pgdata_default}/postgresql.conf" />
</parameter>
</parameters>
<actions>
    <action name="start" timeout="60" />
    <action name="stop" timeout="60" />
    <action name="promote" timeout="30" />
    <action name="demote" timeout="120" />
    <action name="monitor" depth="0" timeout="10" interval="15"/>
    <action name="monitor" depth="0" timeout="10" interval="15" role="Master"/>
    <action name="monitor" depth="0" timeout="10" interval="16" role="Slave"/>
    <action name="notify" timeout="60" />
    <action name="meta-data" timeout="5" />
    <action name="validate-all" timeout="5" />
    <action name="methods" timeout="5" />
</actions>
</resource-agent>""".format(**globals())
OCF_METHODS = """\
start
stop
promote
demote
monitor
notify
methods
meta-data
validate-all"""
ACTION = sys.argv[1] if len(sys.argv) > 1 else None
RS = chr(30)  # record separator
FS = chr(3)  # end of text
_logtag = None
_ocf_nodename = None
_pg_version = None


def get_pguser():
    return os.environ.get('OCF_RESKEY_pguser', pguser_default)


def get_pgbindir():
    return os.environ.get('OCF_RESKEY_pgbindir', pgbindir_default)


def get_pgdata():
    return os.environ.get('OCF_RESKEY_pgdata', pgdata_default)


def get_pghost():
    return os.environ.get('OCF_RESKEY_pghost', pghost_default)


def get_pgport():
    return os.environ.get('OCF_RESKEY_pgport', pgport_default)


def get_pgconf():
    return os.environ.get(
        "OCF_RESKEY_pgconf", os.path.join(get_pgdata(), "postgresql.conf"))


def get_pgctl():
    return os.path.join(get_pgbindir(), "pg_ctl")


def get_psql():
    return os.path.join(get_pgbindir(), "psql")


def get_pgctrldata():
    return os.path.join(get_pgbindir(), "pg_controldata")


def get_pgisready():
    return os.path.join(get_pgbindir(), "pg_isready")


def get_ha_bin():
    return os.environ.get('HA_SBIN_DIR', '/usr/sbin')


def get_ha_debuglog():
    return os.environ.get('HA_DEBUGLOG', "")


def get_crm_master():
    return os.path.join(get_ha_bin(), "crm_master")


def get_crm_node():
    return os.path.join(get_ha_bin(), "crm_node")


def get_logtag():
    global _logtag
    if not _logtag:
        _logtag = "{}({})({})[{}]".format(
            PROGRAM, os.environ['OCF_RESOURCE_INSTANCE'], ACTION, os.getpid())
        if os.environ.get('HA_LOGFACILITY'):
            syslog.openlog(str(_logtag))
    return _logtag


def ocf_log(level, level_str, msg, *args):
    base_l = "{}: {}\n".format(level_str, msg.format(*args))
    full_l = datetime.now().strftime('%b %d %X ') + get_logtag() + ": " + base_l
    if sys.stderr.isatty():
        sys.stderr.write(full_l)
        return 0
    log_facility = os.environ.get('HA_LOGFACILITY')
    if log_facility:  # == "daemon":
        syslog.syslog(level, base_l)
    ha_logfile = os.environ.get("HA_LOGFILE")
    if ha_logfile:
        with open(ha_logfile, "a") as f:
            f.write(full_l)
    elif not log_facility:
        sys.stderr.write(full_l)
    ha_debuglog = get_ha_debuglog()
    if ha_debuglog and ha_debuglog != ha_logfile:
        with open(ha_debuglog, "a") as f:
            f.write(full_l)


log_crit = partial(ocf_log, syslog.LOG_CRIT, "critical")
log_err = partial(ocf_log, syslog.LOG_ERR, "error")
log_warn = partial(ocf_log, syslog.LOG_WARNING, "warning")
log_info = partial(ocf_log, syslog.LOG_INFO, "info")
log_debug = partial(ocf_log, syslog.LOG_DEBUG, "debug")


def get_attrd_updater():
    return os.path.join(get_ha_bin(), "attrd_updater")


def get_ha_private_attr(name, node=None):
    cmd = [get_attrd_updater(), "-Q", "-n", name, "-p"]
    if node:
        cmd.extend(["-N", node])
    try:
        with open(os.devnull, 'w') as null_fh:
            ans = check_output(cmd, stderr=null_fh)
    except CalledProcessError:
        return ""
    p = re.compile(r'^name=".*"\s+host=".*"\s+value="(.*)"$')
    m = p.findall(ans)
    if not m:
        return ""
    return m[0]


def set_ha_private_attr(name, val, node=None):
    cmd = [get_attrd_updater(), "-U", val, "-n", name, "-p", "-d", "0"]
    if node:
        cmd.extend(["-N", node])
    log_cmd(cmd)
    return call(cmd) == 0


def del_ha_private_attr(name):
    # option "-p" must be present, but I don't know why
    cmd = [get_attrd_updater(), "-D", "-n", name, "-p", "-d", "0"]
    log_cmd(cmd)
    with open(os.devnull, "w") as null_fh:
        return call(cmd, stderr=null_fh) == 0


def run_pgctrldata():
    try:
        cmd = [get_pgctrldata(), get_pgdata()]
        log_cmd(cmd)
        return check_output(cmd)
    except CalledProcessError as e:
        return e.output


def log_cmd(cmd, user=None):
    prefix = "as {}: ".format(user) if user else ""
    if isinstance(cmd, list) or isinstance(cmd, tuple):
        cmd = " ".join(pipes.quote(c) for c in cmd)
    log_debug(prefix + cmd.replace(RS, "<RS>").replace(FS, "<FS>"))


def get_ocf_nodename():
    global _ocf_nodename
    if not _ocf_nodename:
        try:
            cmd = [get_crm_node(), "-n"]
            log_cmd(cmd)
            _ocf_nodename = check_output(cmd).strip()
        except CalledProcessError:
            log_and_exit(OCF_ERR_GENERIC)
    return _ocf_nodename


def as_postgres_user():
    u = pwd.getpwnam(get_pguser())
    os.initgroups(get_pguser(), u.pw_gid)
    os.setgid(u.pw_gid)
    os.setuid(u.pw_uid)
    os.seteuid(u.pw_uid)


def as_postgres(cmd):
    cmd = [str(c) for c in cmd]
    log_cmd(cmd, get_pguser())
    with open(os.devnull, "w") as null_fh:
        return call(
            cmd, preexec_fn=as_postgres_user, stdout=null_fh, stderr=STDOUT)


def pg_execute(query):
    try:
        tmp_fh, tmp_file = tempfile.mkstemp(prefix="pgsqlms-")
        os.write(tmp_fh, query)
        os.close(tmp_fh)
        os.chmod(tmp_file, 0o644)
    except:
        log_crit("could not create or write in a temp file")
        log_and_exit(OCF_ERR_INSTALLED)
    try:
        cmd = [
            get_psql(), "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-qXAtf",
            tmp_file, "-R", RS, "-F", FS, "-p", get_pgport(), "-h", get_pghost()]
        log_cmd(cmd, get_pguser())
        ans = check_output(cmd, preexec_fn=as_postgres_user)
    except CalledProcessError as e:
        log_debug("psql error, return code: {}", e.returncode)
        # Possible return codes:
        #  -1: wrong parameters
        #   1: failed to get resources (memory, missing file, ...)
        #   2: unable to connect
        #   3: query failed
        return e.returncode, []
    finally:
        os.remove(tmp_file)
    log_debug("executed query:\n{}", query)
    rs = []
    if ans:
        ans = ans[:-1]
        for record in ans.split(RS):
            rs.append(record.split(FS))
        log_debug("rs: {}", rs)
    return 0, rs


def get_ha_nodes():
    try:
        cmd = [get_crm_node(), "-p"]
        log_cmd(cmd)
        return check_output(cmd).split()
    except CalledProcessError as e:
        log_err("{} failed with return code {}", e.cmd, e.returncode)
        log_and_exit(OCF_ERR_GENERIC)


def set_standbies_scores():
    """ Set scores to better the odds that the standby closest to the master
    will be the selected when Pacemaker chooses a standby to promote. The idea
    is to avoid going through pre-promote and finding out that there is a better
    standby. The promotion would then be canceled, and Pacemaker would promote
    that other standby
    Notes regarding scores:
    - Highest is best
    - Master has score 1001
    - Standbies get 999, 998, ...
    - Bad nodes get -1
    - Unknown nodes get -1000 """
    nodename = get_ocf_nodename()
    nodes_to_score = get_ha_nodes()
    rc, rs = pg_execute(
        "SELECT T1.slot_name, T1.restart_lsn, T1.active_pid, T2.state \n"
        "FROM pg_replication_slots AS T1 "
        "LEFT OUTER JOIN pg_stat_replication AS T2 ON  T1.active_pid=T2.pid\n"
        "WHERE T1.active AND T1.slot_type='physical' \n"
        "ORDER BY T1.restart_lsn DESC")
    if rc != 0:
        log_err("failed to get replication slots info")
        log_and_exit(OCF_ERR_GENERIC)
    # For each standby connected, set their master score based on the following
    # rule: the first known node, with the highest priority and
    # with an acceptable state.
    for i, (slot_name, restart_lsn, active_pid, state) in enumerate(rs):
        for node in nodes_to_score:
            if node.replace('-', '_') == slot_name:
                node_to_score = node
                break
        else:
            log_info("ignoring unknown physical slot {}", slot_name)
            continue
        if state in ("startup", "backup"):
            # exclude standbies in state backup (pg_basebackup) or startup (new
            # standby or failing standby)
            log_info("forbid promotion of {} because it has state '{}': "
                     "set its score to -1", node_to_score, state)
            set_promotion_score("-1", node_to_score)
        else:
            score = 1000 - i - 1
            log_info("update score of {} to {}", node_to_score, score)
            set_promotion_score(score, node_to_score)
        # Remove this node from the known nodes list.
        nodes_to_score.remove(node_to_score)
    # If there are still nodes in "nodes_to_score", it means there is no
    # corresponding line in "pg_stat_replication".
    for node in nodes_to_score:
        if node == nodename:  # Exclude this node
            continue
        log_warn("{} is not connected to the master, set score to -1000", node)
        set_promotion_score("-1000", node)
    set_promotion_score("1001")
    return OCF_SUCCESS


def is_master_or_standby():
    """ Confirm if the instance is really started as pg_isready stated and
    if the instance is master or standby
    Return OCF_SUCCESS or OCF_RUNNING_MASTER or OCF_ERR_GENERIC or
    OCF_ERR_CONFIGURED """
    rc, rs = pg_execute("SELECT pg_is_in_recovery()")
    if rc == 0:
        is_in_recovery = rs[0][0]
        if is_in_recovery == "t":
            log_debug("PG running as a standby")
            return OCF_SUCCESS
        else:  # is_in_recovery == "f":
            log_debug("PG running as a master")
            return OCF_RUNNING_MASTER
    elif rc in (1, 2):
        # psql cound not connect; pg_isready reported PG is listening, so this
        # error could be a max_connection saturation: report a soft error
        log_err("psql can't connect to server")
        return OCF_ERR_GENERIC
    # The query failed (rc: 3) or bad parameters (rc: -1).
    # This should not happen, raise a hard configuration error.
    log_err("query to check if PG is a master or standby failed, rc: {}", rc)
    return OCF_ERR_CONFIGURED


def get_pgctrldata_state():
    """ Parse and return the current status of the local PostgreSQL instance as
    reported by its controldata file
    WARNING: the status is NOT updated in case of crash. """
    datadir = get_pgdata()
    finds = re.findall(RE_PG_CLUSTER_STATE, run_pgctrldata(), re.M)
    if not finds:
        log_crit("couldn't read state from controldata file for {}", datadir)
        log_and_exit(OCF_ERR_CONFIGURED)
    log_debug("PG state is '{}'", finds[0])
    return finds[0]


def pg_ctl_status():
    """ checks whether PG is running
    Return 0 if the server is running, 3 otherwise """
    return as_postgres([get_pgctl(), "status", "-D", get_pgdata()])


def pg_ctl_restart():
    return as_postgres(
        [get_pgctl(), "restart", "-D", get_pgdata(), "-w", "-t", 1000000,
         "-l", "/dev/null"])


def pg_ctl_start():
    # long timeout to ensure Pacemaker gives up first
    return as_postgres(
        [get_pgctl(), "start", "-D", get_pgdata(), "-w", "-t", 1000000,
         "-o", "-c config_file=" + get_pgconf(), "-l", "/dev/null"])


def pg_ctl_stop():
    # long timeout to ensure Pacemaker gives up first
    return as_postgres([get_pgctl(), "stop", "-D", get_pgdata(),
                        "-w", "-t", 1000000, "-m", "fast"])


def backup_label_exists():
    return os.path.isfile(os.path.join(get_pgdata(), "backup_label"))


def create_recovery_conf(master):
    """ Write recovery.conf. It must include:
    standby_mode = on
    primary_conninfo = 'host=<VIP> port=5432 user=repl1'
    recovery_target_timeline = latest
    primary_slot_name = <node_name>
    """
    u = pwd.getpwnam(get_pguser())
    uid, gid = u.pw_uid, u.pw_gid
    this_host = get_ocf_nodename()
    recovery_file = os.path.join(get_pgdata(), "recovery.conf")
    try:
        with open(recovery_file) as fh:
            recovery_conf_old = fh.read()
    except:
        recovery_conf_old = None
    new_conf = ("standby_mode = 'on'\n"
                "recovery_target_timeline = 'latest'\n")
    if master and master != this_host:
        new_conf += (
            "primary_conninfo = 'host={} port={} user={}'\n"
            "primary_slot_name = '{}'\n").format(
                master, get_pgport(), get_pguser(),
                get_ocf_nodename().replace("-", "_"))
    log_debug("previous {}:\n{}", recovery_file, recovery_conf_old)
    if recovery_conf_old == new_conf:
        log_debug("recovery.conf already as wanted, no need to udpate it")
        return False
    log_debug("writing updated {}:\n{}", recovery_file, new_conf)
    try:
        with open(recovery_file, "w") as fh:
            fh.write(new_conf)
    except:
        log_crit("can't open {}", recovery_file)
        log_and_exit(OCF_ERR_CONFIGURED)
    try:
        os.chown(recovery_file, uid, gid)
    except:
        log_crit("can't set owner of {}", recovery_file)
        log_and_exit(OCF_ERR_CONFIGURED)
    return True


def get_promotion_score(node=None):
    cmd = [get_crm_master(), "--lifetime", "forever", "--quiet", "--get-value"]
    if node:
        cmd.extend(["-N", node])
    try:
        log_cmd(cmd)
        with open(os.devnull, 'w') as null_fh:
            score = check_output(cmd, stderr=null_fh)
    except CalledProcessError:
        return ""
    return score.strip()


def no_node_has_a_positive_score():
    for node in get_ha_nodes():
        try:
            if int(get_promotion_score(node)) >= 0:
                return False
        except ValueError:
            pass
    return True


def set_promotion_score(score, node=None):
    score = str(score)
    cmd = [get_crm_master(), "--lifetime", "forever", "-q", "-v", score]
    if node:
        cmd.extend(["-N", node])
    log_cmd(cmd)
    call(cmd)
    # setting attributes is asynchronous, so return as soon as truly done
    while True:
        tmp = get_promotion_score(node)
        if tmp == score:
            break
        log_debug("waiting to set score to {} (currently {})...", score, tmp)
        sleep(0.1)


def ocf_promote():
    state = is_master_or_standby()
    if state == OCF_RUNNING_MASTER:
        log_warn("PG already running as a master")
        return OCF_SUCCESS
    elif state != OCF_SUCCESS:
        log_err("Unexpected error, cannot promote this node")
        return OCF_ERR_GENERIC
    # Cancel if it has been considered unsafe during pre-promote
    if get_ha_private_attr("cancel_promotion") == "1":
        log_err("Promotion was cancelled during pre-promote")
        del_ha_private_attr("cancel_promotion")
        return OCF_ERR_GENERIC
    active_nodes = get_ha_private_attr("active_nodes").split()
    # The best standby to promote has the highest LSN. If this node
    # is not the best one, we need to modify the master scores accordingly,
    # and abort the current promotion
    if not is_this_node_best_promotion_candidate(active_nodes):
        return OCF_ERR_GENERIC
    # replication slots can be added on hot standbies, helps ensuring no WAL is
    # missed upon promotion
    slave_nodes = [n for n in active_nodes if n != get_ocf_nodename()]
    if not add_replication_slots(slave_nodes):
        log_err("failed to add all replication slots")
        return OCF_ERR_GENERIC
    if as_postgres([get_pgctl(), "promote", "-D", get_pgdata()]) != 0:
        # Promote the instance on the current node.
        log_err("'pg_ctl promote' failed")
        return OCF_ERR_GENERIC
    # promote is asynchronous: wait for it to finish, at least with PG 9.6
    while is_master_or_standby() != OCF_RUNNING_MASTER:
        log_debug("waiting for promote to complete")
        sleep(1)
    log_info("promote complete")
    return OCF_SUCCESS


def add_replication_slots(slave_nodes):
    nodename = get_ocf_nodename()
    rc, rs = pg_execute("SELECT slot_name FROM pg_replication_slots")
    slots = [r[0] for r in rs]
    for node in slave_nodes:
        if node == nodename:
            continue  # TODO: is this check necessary?
        slot = node.replace('-', '_')
        if slot in slots:
            log_debug("replication slot '{}' exists already".format(slot))
            continue
        rc, rs = pg_execute(
            "SELECT pg_create_physical_replication_slot('{}', true)".format(
                slot))
        if rc != 0:
            log_err("failed to add replication slot '{}'".format(slot))
            return False
        log_debug("added replication slot '{}'".format(slot))
    return True


def kill_wal_sender(slot):
    rc, rs = pg_execute(
        "SELECT pg_terminate_backend(active_pid) "
        "FROM pg_replication_slots "
        "WHERE slot_name='{}' AND active_pid IS NOT NULL".format(slot))
    if rs:
        log_info("killed active wal sender for slot {}".format(slot))


def kill_wal_senders(slave_nodes):
    rc, rs = pg_execute("SELECT slot_name "
                        "FROM pg_replication_slots "
                        "WHERE active_pid IS NOT NULL")
    slots = [r[0] for r in rs]
    for node in slave_nodes:
        slot = node.replace('-', '_')
        if slot in slots:
            kill_wal_sender(slot)


def delete_replication_slots():
    rc, rs = pg_execute("SELECT slot_name FROM pg_replication_slots")
    if not rs:
        log_debug("no replication slots to delete")
        return True
    for slot in [r[0] for r in rs]:
        kill_wal_sender(slot)
        q = "SELECT pg_drop_replication_slot('{}')".format(slot)
        rc, rs = pg_execute(q)
        if rc != 0:
            log_err("failed to delete replication slot {}".format(slot))
            return False
        log_debug("deleted replication slot {}".format(slot))
    return True


def is_this_node_best_promotion_candidate(active_nodes):
    """ Find out if this node is truly the one to promote. If not, also update
    the scores.
    The standby with the highest master score gets promoted by pacemaker, as set
    by the master during monitor (see set_standbies_scores). Confirm that
        - all standbies have set "wal_lsn" during pre-promote; and
        - this standby has the highest "wal_lsn"
    Return True if this node is to be promoted, False otherwise """
    log_debug("checking that this node is the best candidate for promotion")
    # Exclude nodes that are not in the current partition
    local_node = get_ocf_nodename()
    node_to_promote = local_node
    local_lsn = get_ha_private_attr("wal_lsn")  # set during pre-promote
    if local_lsn == "":
        log_crit("this node isn't the best candidate: no WAL LSN")
        return False
    # convert LSN to a pair of integers
    local_lsn = [int(v, 16) for v in local_lsn.split("/")]
    highest_lsn = local_lsn
    log_debug("WAL LSN of this node: {}", local_lsn)
    # Now we compare with the other available nodes.
    for node in (n for n in active_nodes if n != local_node):
        node_lsn = get_ha_private_attr("wal_lsn", node)
        if node_lsn == "":
            log_crit("can't confirm this node is the best candidate: "
                     "no WAL LSN set for {}", node)
            return False
        # convert LSN to decimal
        node_lsn = [int(v, 16) for v in node_lsn.split("/")]
        log_debug("WAL LSN for {}: {}", node, node_lsn)
        if node_lsn > highest_lsn:
            node_to_promote = node
            highest_lsn = node_lsn
            log_debug("{}'s WAL LSN is higher", node)
    if node_to_promote != local_node:
        log_info("{} is the best candidate, aborting promotion",
                 node_to_promote)
        set_promotion_score("1")  # minimize local node's score
        set_promotion_score("1000", node_to_promote)
        # Make promotion fail; another one will occur with updated scores
        return False
    log_info("this node is the best candidate, proceeding with promotion")
    return True


def del_private_attributes():
    del_ha_private_attr("wal_lsn")
    del_ha_private_attr("active_nodes")
    del_ha_private_attr("cancel_promotion")


def get_lsn():
    if get_pg_version() >= LooseVersion("10"):
        q = ("SELECT GREATEST("
             "pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn())")
    else:
        q = ("SELECT GREATEST("
             "pg_last_xlog_receive_location(), pg_last_xlog_replay_location())")
    rc, rs = pg_execute(q)
    if rc != 0:
        log_warn("could not get WAL LSN")
        return None
    return rs[0][0]


def notify_pre_promote(nodes):
    this_node = get_ocf_nodename()
    if nodes["promote"] and nodes["master"]:
        log_warn("looks like the previous master is being promoted back")
    del_private_attributes()

    # FIXME: should we allow a switchover to a lagging standby?

    # Run an election among slaves to promote the best one based on WAL LSNs.
    # This information is used during ocf_promote to check if the promoted node
    # is the best one. If not, the promotion will be "cancelled" (which will
    # allow the best slave to be promoted subsequently)
    node_lsn = get_lsn()
    if node_lsn is None:
        return
    log_info("WAL LSN: {}", node_lsn)
    # Set the "wal_lsn" attribute value for this node so we can use it
    # during the following "promote" action.
    if not set_ha_private_attr("wal_lsn", node_lsn):
        log_warn("could not set the WAL LSN")
    # If this node is the future master, keep track of the slaves that received
    # the same notification to compare our LSN with them during promotion
    if this_node in nodes["promote"]:
        active_nodes = (
            (nodes["master"] - nodes["demote"]) |  # master
            (((nodes["slave"] | nodes["demote"]) - nodes["stop"]) | nodes["start"]))  # slaves
        set_ha_private_attr("active_nodes", " ".join(active_nodes))


def notify_pre_start(nodes):
    """ If this is a Master:
        - add any missing replication slots
        - kill any active orphaned wal sender
    Return OCF_SUCCESS (sole possibility AFAICT) """
    this_node = get_ocf_nodename()
    if this_node in nodes["master"] and this_node not in nodes["demote"]:
        add_replication_slots(nodes["start"])
        kill_wal_senders(nodes["start"])
        for node in nodes["start"]:
            if node == this_node:
                continue
            log_debug("setting attr 'prestart_master' as {} for node {}",
                      this_node, node)
            set_ha_private_attr("prestart_master", this_node, node)


def notify_post_start(nodes):
    del_ha_private_attr("prestart_master")


def get_pg_version():
    global _pg_version
    if not _pg_version:
        ver_file = os.path.join(get_pgdata(), "PG_VERSION")
        try:
            with open(ver_file) as fh:
                ver = fh.read()
        except Exception as e:
            log_crit("Can't read PG version file: {}", ver_file, e)
            log_and_exit(OCF_ERR_ARGS)
        try:
            _pg_version = LooseVersion(ver)
        except:
            log_crit("Can't parse PG version: {}", ver)
            log_and_exit(OCF_ERR_ARGS)
    return _pg_version


def ocf_validate_all():
    if int(os.environ.get("OCF_RESKEY_CRM_meta_clone_max", 0)) <= 0:
        log_err("OCF_RESKEY_CRM_meta_clone_max should be >= 1")
        log_and_exit(OCF_ERR_CONFIGURED)
    if os.environ.get("OCF_RESKEY_CRM_meta_master_max", "") != "1":
        log_err("OCF_RESKEY_CRM_meta_master_max should == 1")
        log_and_exit(OCF_ERR_CONFIGURED)
    for prog in [get_pgctl(), get_psql(), get_pgisready(), get_pgctrldata()]:
        if not os.access(prog, os.X_OK):
            log_crit("{} is missing or not executable", prog)
            log_and_exit(OCF_ERR_INSTALLED)
    datadir = get_pgdata()
    if not os.path.isdir(datadir):
        log_err("PGDATA {} not found".format(datadir))
        log_and_exit(OCF_ERR_ARGS)
    ver = get_pg_version()
    if ver < MIN_PG_VER:
        log_err("PostgreSQL {} is too old: >= {} required", ver, MIN_PG_VER)
        log_and_exit(OCF_ERR_INSTALLED)
    try:
        pwd.getpwnam(get_pguser())
    except KeyError:
        log_crit("System user {} does not exist", get_pguser())
        log_and_exit(OCF_ERR_ARGS)
    return OCF_SUCCESS


def ocf_start():
    """ Start as a hot standby """
    rc = get_ocf_state()
    # Instance must be stopped or running as standby
    if rc == OCF_SUCCESS:
        log_info("PG is already started as a standby")
        return OCF_SUCCESS
    if rc != OCF_NOT_RUNNING:
        log_err("unexpected PG state: {}", rc)
        return OCF_ERR_GENERIC
    return start_pg_as_standby(get_ha_private_attr("prestart_master"))


def start_pg_as_standby(master):
    # From here, the instance is NOT running
    prev_state = get_pgctrldata_state()
    log_debug("starting PG as a standby")
    create_recovery_conf(master)
    rc = pg_ctl_start()
    if rc != 0:
        log_err("failed to start PG, rc: {}", rc)
        return OCF_ERR_GENERIC
    if is_master_or_standby() != OCF_SUCCESS:
        log_err("PG is not running as a standby (returned {})", rc)
        return OCF_ERR_GENERIC
    log_info("PG started")
    # On first cluster start, no score exists on standbies unless someone sets
    # them with crm_master. Without scores, the cluster won't promote any slave.
    # Fix that by setting a score of 1, but only if this node was a master
    # before, hence picking the node that was used as master during setup.
    if prev_state == "shut down" and no_node_has_a_positive_score():
        log_info("no master score around; set mine to 1")
        set_promotion_score("1")
    return OCF_SUCCESS


def ocf_stop():
    """ Return OCF_SUCCESS or OCF_ERR_GENERIC """
    rc = get_ocf_state()
    if rc == OCF_NOT_RUNNING:
        log_info("PG already stopped")
        return OCF_SUCCESS
    elif rc not in (OCF_SUCCESS, OCF_RUNNING_MASTER):
        log_warn("unexpected PG state: {}", rc)
        return OCF_ERR_GENERIC
    # From here, the instance is running for sure
    log_debug("PG running, stopping it")
    # Try to quit with proper shutdown (won't return if stop never finishes)
    if pg_ctl_stop() == 0:
        log_info("PG stopped")
        return OCF_SUCCESS
    log_err("PG failed to stop")
    return OCF_ERR_GENERIC


def run_pgisready():
    """
    pg_isready returns
        0 to the shell if the server is accepting connections normally,
        1 if the server is rejecting connections (for example during startup),
        2 if there was no response to the connection attempt, and
        3 if no attempt was made (for example due to invalid parameters).
    """
    return as_postgres([
        get_pgisready(), "-h", get_pghost(), "-p", get_pgport(), "-q"])


def loop_while_pg_rejects_connections():
    """ return True if PG accepts connections
        return False if PG is mute to connection attempts """
    while True:
        rc = run_pgisready()
        if rc == 3:
            log_err("pg_isready reports invalid parameters, "
                    "or something of the sort (exit status 3)")
            log_and_exit(OCF_ERR_CONFIGURED)
        if rc == 1:
            sleep(0.5)
            continue
        if rc == 0:
            return True
        return False  # should mean rc == 2 (see pg_isready docs)


def ocf_monitor():
    if loop_while_pg_rejects_connections():
        log_debug("PG is listening")
        status = is_master_or_standby()
        if status == OCF_RUNNING_MASTER:
            set_standbies_scores()
        return status
    log_info("PG is not listening; check if it is running with 'pg_ctl status'")
    if pg_ctl_status() == 0:
        log_err("PG is running yet it isn't listening")
        return OCF_ERR_GENERIC
    log_info("PG isn't running")
    if backup_label_exists():
        log_debug("found backup_label: indicates a never started backup")
        return OCF_NOT_RUNNING
    log_debug("backup_label not found: check pg_controldata's state "
              "to find out if PG was shutdown gracefully")
    state = get_pgctrldata_state()
    log_info("pg_controldata's state is: {}", state)
    if state in ("shut down",  # was a Master
                 "shut down in recovery"  # was a standby
                 ):
        log_debug("shutdown was graceful")
    else:
        log_warn("shutdown was not graceful")
    return OCF_NOT_RUNNING


def get_ocf_state():
    """ Return OCF_SUCCESS or OCF_RUNNING_MASTER if PG is running and accepts
    connections,
    OCF_NOT_RUNNING if PG is not running, and
    OCF_ERR_GENERIC if PG is running but doesn't accept or reject connections
    """
    if loop_while_pg_rejects_connections():
        log_debug("PG is listening")
        return is_master_or_standby()
    log_debug("PG is not listening")
    pgctlstatus_rc = pg_ctl_status()
    if pgctlstatus_rc == 0:
        log_err("PG is running but not listening")
        return OCF_ERR_GENERIC
    return OCF_NOT_RUNNING


def notify_pre_demote(nodes):
    # do nothing if the local node will not be demoted
    node = get_ocf_nodename()
    if node not in nodes["demote"]:
        return
    rc = get_ocf_state()
    # do nothing if this is not a master recovery
    master_recovery = node in nodes["master"] and node in nodes["promote"]
    if not master_recovery or rc != OCF_FAILED_MASTER:
        return
    # in case of master crash, we need to detect if the CRM tries to recover
    # the master clone. The usual transition is to do:
    #   demote->stop->start->promote
    #
    # There are multiple flaws with this transition:
    #  * the 1st and 2nd actions will fail because the instance is in
    #    OCF_FAILED_MASTER step
    #  * the usual start action is dangerous as the instance will start with
    #    a recovery.conf instead of entering a normal recovery process
    #
    # To avoid this, we try to start the instance in recovery from here.
    # If it succeeds, at least it will be demoted correctly with a normal
    # status.
    # pg_ctl '-w' ensures the OCF action fails by timeout if the start takes too
    # long. If it couldn't start, this failure will be picked up later on.
    log_info("trying to start failed PG master")
    pg_ctl_start()
    log_info("state is '{}' after recovery attempt", get_pgctrldata_state())


def ocf_demote():
    """ Demote PG from master to standby: stop PG, create recovery.conf and
    start it back """
    rc = get_ocf_state()
    if rc == OCF_RUNNING_MASTER:
        log_info("Normal demotion")
        rc = pg_ctl_stop()
        if rc != 0:
            log_err("failed to stop PG (pg_ctl exited with {})", rc)
            return OCF_ERR_GENERIC
        # Double check that PG is stopped correctly
        rc = get_ocf_state()
        if rc != OCF_NOT_RUNNING:
            log_err("unexpected state: monitor status "
                    "({}) disagrees with pg_ctl return code", rc)
            return OCF_ERR_GENERIC
    elif rc == OCF_SUCCESS:
        return OCF_SUCCESS
    elif rc == OCF_NOT_RUNNING:
        log_warn("PG is already stopped")  # an error should be returned
        # return OCF_ERR_GENERIC
    elif rc == OCF_ERR_CONFIGURED:
        # We prefer raising a hard error instead of letting the CRM abort and
        # start another transition due to a soft error.
        # The hard error will force the CRM to move the resource immediately.
        return OCF_ERR_CONFIGURED
    else:
        return OCF_ERR_GENERIC
    rc = start_pg_as_standby(master=None)
    if rc == OCF_SUCCESS:
        log_info("PG started as a standby")
        return OCF_SUCCESS
    log_err("failed to start PG as a standby (returned {})", rc)
    return OCF_ERR_GENERIC


def notify_post_promote(nodes):
    del_private_attributes()
    this_node = get_ocf_nodename()
    promoted = nodes["promote"].copy().pop()
    slaves = (((nodes["slave"] | nodes["demote"]) - nodes["stop"]) | nodes["start"]) - nodes["promote"]
    if this_node in slaves:
        # shed any replication slot
        delete_replication_slots()
        # restart only if master changed
        if not create_recovery_conf(promoted):
            log_info("PG already replicates from the current master, "
                     "no need to restart it")
            return
        rc = pg_ctl_restart()
        if rc != 0:
            log_err("failed to restart PG, rc: {}", rc)
            return
        if is_master_or_standby() != OCF_SUCCESS:
            log_err("PG is not running as a standby (returned {})", rc)
            return
        log_info("PG re-started")
    elif this_node == promoted:
        if is_master_or_standby() != OCF_RUNNING_MASTER:
            log_err("PG is not running as a master")
            return
        if not slaves:
            return
        slots = ", ".join("'" + s.replace('-', '_') + "'" for s in slaves)
        q = ("SELECT slot_name "
             "FROM pg_replication_slots "
             "WHERE slot_name in ({}) AND active AND slot_type='physical' "
             "ORDER BY restart_lsn").format(slots)
        while True:
            rc, rs = pg_execute(q)
            if rc != 0:
                log_err("failed to get replication slots info")
                return
            elif len(rs) == len(slaves):
                break
            log_debug("Not all standbies are replicating: expecting {}, got {}",
                      slots, rs)
            sleep(1)
        log_info("all standbies are replicating, set their promotion scores")
        set_standbies_scores()


def get_nofify_unames():
    d = dict(all=[], inactive=[], available=[], slave=[], master=[],
             stop=[], start=[], demote=[], promote=[])
    start, end = "OCF_RESKEY_CRM_meta_notify_", "_uname"
    for k, v in os.environ.items():
        if k.startswith(start) and k.endswith(end):
            action = k[len(start):-len(end)]
            if v.strip():
                if action not in d:
                    log_warn("unknown action in {}", k)
                else:
                    d[action].extend(v.split())
    return d


def ocf_notify():
    # http://clusterlabs.org/doc/en-US/Pacemaker/1.1/html/Pacemaker_Explained/_proper_interpretation_of_multi_state_notification_environment_variables.html
    type_op = (os.environ.get("OCF_RESKEY_CRM_meta_notify_type", "") + "-" +
               os.environ.get("OCF_RESKEY_CRM_meta_notify_operation", ""))
    unames = get_nofify_unames()
    log_debug("{}:\n{}", type_op, json.dumps(
        {k: v for k, v in unames.iteritems() if v}, indent=2, sort_keys=True))
    for state in list(unames):
        unames[state] = set(unames[state])
    if type_op == "pre-promote":
        notify_pre_promote(unames)
    elif type_op == "post-promote":
        notify_post_promote(unames)
    elif type_op == "pre-demote":
        notify_pre_demote(unames)
    elif type_op == "pre-start":
        notify_pre_start(unames)
    elif type_op == "post-start":
        notify_post_start(unames)
    return log_and_exit(OCF_SUCCESS)


def log_and_exit(code):
    log_info("{} exiting with {} ({})", ACTION, OCF_EXIT_CODES[code], code)
    sys.exit(code)


if __name__ == "__main__":
    if ACTION == "meta-data":
        print(OCF_META_DATA)
        sys.exit()
    if ACTION == "methods":
        print(OCF_METHODS)
        sys.exit()
    os.chdir(gettempdir())
    if ACTION == "validate-all":
        log_and_exit(ocf_validate_all())
    if ACTION in ("start", "stop", "monitor", "promote", "demote", "notify"):
        ocf_validate_all()
        if ACTION == "start":  # start as standby
            log_and_exit(ocf_start())
        if ACTION == "stop":
            log_and_exit(ocf_stop())
        if ACTION == "monitor":
            log_and_exit(ocf_monitor())
        if ACTION == "promote":
            log_and_exit(ocf_promote())
        if ACTION == "demote":
            log_and_exit(ocf_demote())
        if ACTION == "notify":
            log_and_exit(ocf_notify())
    else:
        sys.exit(OCF_ERR_UNIMPLEMENTED)
