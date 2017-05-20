#!/usr/bin/python2.7
import os
import pwd
import re
import subprocess
import unittest
import xml.etree.ElementTree as ET
from distutils.version import StrictVersion
from tempfile import gettempdir
from subprocess import check_call, call

import pgha as RA


# returns CENTOS, UBUNTU, DEBIAN, or something else
def get_distro():
    with open("/etc/os-release") as f:
        s = f.read()
    p = re.compile(r'^ID="?(?P<distro>\w+)"?', re.M)
    return p.findall(s)[0].upper()


pgversion = "9.6"
INST = "foo"
os.environ['OCF_RESOURCE_INSTANCE'] = INST
linux_distro = get_distro()
if linux_distro == "CENTOS":
    PGBINDIR = "/usr/pgsql-{}/bin".format(pgversion)
elif linux_distro in ("DEBIAN", "UBUNTU"):
    PGBINDIR = "/usr/lib/postgresql/{}/bin".format(pgversion)
else:
    raise Exception("unknown linux distro: " + linux_distro)
os.environ['OCF_RESKEY_pgbindir'] = PGBINDIR
os.chdir(gettempdir())

PGUSER = "postgres"
PGUSER_UID, PGUSER_GID = pwd.getpwnam(PGUSER)[2:4]
UNAME = subprocess.check_output("uname -n", shell=True).strip()
PGDATA_MASTER = "/tmp/pgsqlha_test_master"
PGDATA_SLAVE = "/tmp/pgsqlha_test_slave"
PGPORT_MASTER = "15432"
PGPORT_SLAVE = "15433"
PGSTART_OPTS = "'-c config_file={}/postgresql.conf'"
TPL = """\
standby_mode = on
recovery_target_timeline = latest
primary_conninfo = 'host=/var/run/postgresql port={} user={}'
primary_slot_name = '{}'
""".format(PGPORT_MASTER, PGUSER, RA.get_ocf_nodename().replace("-", "_"))
CONF_ADDITIONS = """
listen_addresses = '*'
port = {}
wal_level = replica
max_wal_senders = 5
hot_standby = on
hot_standby_feedback = on
max_replication_slots = 10
"""
HBA_ADDITIONS = "\nlocal replication all trust\n"


def change_user_to_postgres():
    os.initgroups(PGUSER, PGUSER_GID)
    os.setgid(PGUSER_GID)
    os.setuid(PGUSER_UID)
    os.seteuid(PGUSER_UID)


def write_as_pg(f, s):
    with open(f, "w") as fh:
        fh.write(s)
    os.chown(f, PGUSER_UID, PGUSER_GID)


def run_as_pg(cmd, check=True):
    print("run_as_pg: " + cmd)
    to_call = check_call if check else call
    return to_call(
        cmd, shell=True, cwd="/tmp", preexec_fn=change_user_to_postgres)


class TestPg(unittest.TestCase):

    @classmethod
    def start_pg(cls, pgdata):
        run_as_pg("{}/pg_ctl start -D {} -o {} -w".format(
            PGBINDIR, pgdata, PGSTART_OPTS.format(pgdata)))

    @classmethod
    def start_pg_master(cls):
        cls.start_pg(PGDATA_MASTER)

    @classmethod
    def start_pg_slave(cls):
        cls.start_pg(PGDATA_SLAVE)

    @classmethod
    def stop_pg(cls, pgdata):
        run_as_pg("{}/pg_ctl stop -D {} -w".format(PGBINDIR, pgdata))

    @classmethod
    def stop_pg_master(cls):
        cls.stop_pg(PGDATA_MASTER)

    @classmethod
    def stop_pg_slave(cls):
        cls.stop_pg(PGDATA_SLAVE)

    @classmethod
    def crash_stop_pg(cls, pgdata):
        run_as_pg("{}/pg_ctl stop -D {} -w -m immediate".format(PGBINDIR, pgdata))

    @classmethod
    def cleanup(cls):
        for datadir in (PGDATA_MASTER, PGDATA_SLAVE):
            try:
                cls.crash_stop_pg(datadir)
            except subprocess.CalledProcessError:
                pass
        run_as_pg("rm -rf {}".format(PGDATA_MASTER))
        run_as_pg("rm -rf {}".format(PGDATA_SLAVE))

    @classmethod
    def setUpClass(cls):
        cls.cleanup()
        # create master instance
        run_as_pg("mkdir -p -m 0700 {}".format(PGDATA_MASTER))
        run_as_pg("{}/initdb -D {}".format(PGBINDIR, PGDATA_MASTER))
        with open(PGDATA_MASTER + "/postgresql.conf", "a") as fh:
            fh.write(CONF_ADDITIONS.format(PGPORT_MASTER))
        with open(PGDATA_MASTER + "/pg_hba.conf", "a") as fh:
            fh.write(HBA_ADDITIONS)
        cls.start_pg_master()
        # backup master to slave and set it up as a standby
        run_as_pg("mkdir -p -m 0700 {}".format(PGDATA_SLAVE))
        run_as_pg("pg_basebackup -D {} -d 'port={}' -Xs".format(
            PGDATA_SLAVE, PGPORT_MASTER))
        run_as_pg(
            "sed -i s/{}/{}/ {}".format(
                PGPORT_MASTER,
                PGPORT_SLAVE,
                PGDATA_SLAVE + "/postgresql.conf"))
        write_as_pg(PGDATA_SLAVE + "/recovery.conf", """\
standby_mode = on
recovery_target_timeline = latest
primary_conninfo = 'port={}'
""".format(PGPORT_MASTER))
        cls.start_pg_slave()

    @classmethod
    def tearDownClass(cls):
        cls.cleanup()

    def setUp(self):
        RA.OCF_ACTION = None
        if 'OCF_RESKEY_CRM_meta_interval' in os.environ:
            del os.environ['OCF_RESKEY_CRM_meta_interval']
        try:
            self.stop_pg_slave()
        except subprocess.CalledProcessError:
            pass
        run_as_pg("rm -f " + RA.get_recovery_pcmk(), False)
        os.environ['OCF_RESKEY_start_opts'] = (
            '-c config_file={}/postgresql.conf').format(PGDATA_SLAVE)
        # os.environ['OCF_RESKEY_pghost'] = "localhost"
        self.set_env_to_slave()

    @classmethod
    def set_env_to_master(cls):
        os.environ['OCF_RESKEY_pgdata'] = PGDATA_MASTER
        os.environ['OCF_RESKEY_pgport'] = PGPORT_MASTER

    @classmethod
    def set_env_to_slave(cls):
        os.environ['OCF_RESKEY_pgdata'] = PGDATA_SLAVE
        os.environ['OCF_RESKEY_pgport'] = PGPORT_SLAVE

    def test_ra_action_start(self):
        self.assertRaises(SystemExit, RA.ocf_start)
        with self.assertRaises(SystemExit) as cm:
            RA.ocf_start()
        self.assertEqual(cm.exception.code, RA.OCF_ERR_CONFIGURED)
        write_as_pg(RA.get_recovery_pcmk(), TPL)
        self.assertEqual(RA.OCF_SUCCESS, RA.ocf_start())
        # check if it is connecting to the master
        self.set_env_to_master()

    def test_ra_action_stop(self):
        self.assertEqual(RA.OCF_SUCCESS, RA.ocf_stop())
        self.start_pg_slave()
        rc = RA.run_pgisready()
        self.assertEqual(0, rc)
        self.assertEqual(RA.OCF_SUCCESS, RA.ocf_stop())

    def test_run_pgisready(self):
        rc = RA.run_pgisready()
        self.assertEqual(2, rc)
        self.start_pg_slave()
        rc = RA.run_pgisready()
        self.assertEqual(0, rc)

    def test_confirm_stopped(self):
        self.assertEqual(RA.OCF_NOT_RUNNING, RA.confirm_stopped(INST))
        self.start_pg_slave()
        self.assertEqual(RA.OCF_ERR_GENERIC, RA.confirm_stopped(INST))

    def test_get_pg_ctl_status(self):
        self.assertEqual(3, RA.pg_ctl_status())
        self.start_pg_slave()
        self.assertEqual(0, RA.pg_ctl_status())
        self.stop_pg_slave()
        self.assertEqual(3, RA.pg_ctl_status())

    def test_get_pg_cluster_state(self):
        state = RA.get_pgctrldata_state()
        self.assertEqual('shut down in recovery', state)
        self.start_pg_slave()
        state = RA.get_pgctrldata_state()
        self.assertEqual('in archive recovery', state)
        self.stop_pg_slave()
        state = RA.get_pgctrldata_state()
        self.assertEqual('shut down in recovery', state)

    def test_pg_execute(self):
        self.start_pg_slave()
        rc, rs = RA.pg_execute("SELECT pg_last_xlog_receive_location()")
        self.assertEqual(0, rc)
        rc, rs = RA.pg_execute("SELECT 25")
        self.assertEqual(0, rc)
        self.assertEqual([["25"]], rs)
        rc, rs = RA.pg_execute("this is not a valid query")
        self.assertEqual(3, rc)
        self.assertEqual([], rs)

    def test_get_ocf_status_from_pg_cluster_state(self):
        ocf_status = RA.get_non_transitional_pg_state(INST)
        self.assertEqual(ocf_status, RA.OCF_NOT_RUNNING)
        self.start_pg_slave()
        ocf_status = RA.get_non_transitional_pg_state(INST)
        self.assertEqual(ocf_status, RA.OCF_SUCCESS)

    def test_validate_all(self):
        with self.assertRaises(SystemExit) as cm:
            RA.ocf_validate_all()
        self.assertEqual(cm.exception.code, RA.OCF_ERR_ARGS)
        write_as_pg(RA.get_recovery_pcmk(), TPL)
        self.assertEqual(RA.OCF_SUCCESS, RA.ocf_validate_all())
        try:
            os.environ['OCF_RESKEY_pgbindir'] = "foo"
            with self.assertRaises(SystemExit) as cm:
                RA.ocf_validate_all()
            self.assertEqual(cm.exception.code, RA.OCF_ERR_INSTALLED)
        finally:
            os.environ['OCF_RESKEY_pgbindir'] = PGBINDIR
        self.assertEqual(RA.OCF_SUCCESS, RA.ocf_validate_all())
        try:
            os.environ['OCF_RESKEY_pguser'] = "bar"
            with self.assertRaises(SystemExit) as cm:
                RA.ocf_validate_all()
            self.assertEqual(cm.exception.code, RA.OCF_ERR_ARGS)
        finally:
            os.environ['OCF_RESKEY_pguser'] = RA.pguser_default
        self.assertEqual(RA.OCF_SUCCESS, RA.ocf_validate_all())
        ver_backup = RA.MIN_PG_VER
        try:
            RA.MIN_PG_VER = StrictVersion("100.1")
            with self.assertRaises(SystemExit) as cm:
                RA.ocf_validate_all()
            self.assertEqual(cm.exception.code, RA.OCF_ERR_INSTALLED)
        finally:
            RA.MIN_PG_VER = ver_backup
        self.assertEqual(RA.OCF_SUCCESS, RA.ocf_validate_all())

    def test_add_delete_replication_slots(self):
        self.set_env_to_master()
        RA.set_ha_private_attr("nodes", "node1 node2 {}".format(UNAME))
        self.assertTrue(RA.add_replication_slots(["node1", UNAME, "node2"]))
        rc, rs = RA.pg_execute("SELECT slot_name FROM pg_replication_slots")
        self.assertEqual(0, rc)
        self.assertEqual([["node1"], ["node2"]], rs)
        RA.del_ha_private_attr("nodes")
        self.assertTrue(RA.delete_replication_slots())
        rc, rs = RA.pg_execute("SELECT slot_name FROM pg_replication_slots")
        self.assertEqual(0, rc)
        self.assertEqual([], rs)


class TestHa(unittest.TestCase):

    def setUp(self):
        RA.OCF_ACTION = None
        if 'OCF_RESKEY_CRM_meta_interval' in os.environ:
            del os.environ['OCF_RESKEY_CRM_meta_interval']

    def test_get_set_promotion_score(self):
        RA.set_promotion_score(2, UNAME)
        self.assertEqual("2", RA.get_promotion_score(UNAME))

    def test_get_ha_nodes(self):
        nodes = RA.get_ha_nodes()
        self.assertIn(UNAME, nodes)

    def test_get_ocf_nodename(self):
        n = RA.get_ocf_nodename()
        self.assertEqual(n, UNAME)

    def test_ha_private_attr(self):
        RA.del_ha_private_attr("bla")
        self.assertEqual("", RA.get_ha_private_attr("bla"))
        RA.set_ha_private_attr("bla", "1234")
        n = RA.get_ocf_nodename()
        self.assertEqual("1234", RA.get_ha_private_attr("bla"))
        self.assertEqual("1234", RA.get_ha_private_attr("bla", n))
        self.assertEqual("", RA.get_ha_private_attr("bla", n + "foo"))
        RA.del_ha_private_attr("bla")
        self.assertEqual("", RA.get_ha_private_attr("bla"))

    def test_ocf_meta_data(self):
        s = RA.OCF_META_DATA
        self.assertTrue(len(s) > 100)
        root = ET.fromstring(s)
        self.assertEqual(root.attrib["name"], "pgsqlha")

    def test_ocf_methods(self):
        methods = RA.OCF_METHODS.split("\n")
        self.assertEqual(len(methods), 9)
        self.assertIn("notify", methods)

    def test_get_notify_dict(self):
        os.environ['OCF_RESKEY_CRM_meta_notify_type'] = "pre"
        os.environ['OCF_RESKEY_CRM_meta_notify_operation'] = "promote"
        os.environ['OCF_RESKEY_CRM_meta_notify_start_uname'] = "node1 node2"
        os.environ['OCF_RESKEY_CRM_meta_notify_stop_uname'] = "node3 node4"
        d = RA.get_notify_dict()
        self.assertEqual(["node1", "node2"], d["nodes"]["start"])
        import json
        print(json.dumps(d, indent=4))


class TestRegExes(unittest.TestCase):
    TPL = """\
primary_conninfo = 'user={} host=127.0.0.1 port=15432'
recovery_target_timeline = 'latest'
primary_slot_name = 'foo'
""".format(PGUSER)
    cluster_state = """\
Catalog version number:               201608131
Database system identifier:           6403755386726595987
Database cluster state:               in production
pg_control last modified:             Fri 31 Mar 2017 10:01:29 PM CEST
"""

    def test_tpl_file(self):
        m = re.search(RA.RE_TPL_TIMELINE, TestRegExes.TPL, re.M)
        self.assertIsNotNone(m)

    def test_tpl_slot(self):
        finds = re.findall(RA.RE_TPL_SLOT, TestRegExes.TPL, re.M)
        self.assertAlmostEqual(finds[0], "foo")

    def test_pg_cluster_state(self):
        m = re.findall(RA.RE_PG_CLUSTER_STATE, TestRegExes.cluster_state, re.M)
        self.assertIsNotNone(m)
        self.assertEqual(m[0], "in production")

if __name__ == '__main__':
    unittest.main()
