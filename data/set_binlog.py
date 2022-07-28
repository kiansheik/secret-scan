#!/bin/python3
# This script will set up binlog replication from one cluster to another which has been recently restored from a snapshot of the other
# First argument is source cluster identifier. Second argument is destination cluster identifier
# Destination cluster has been restored from a snapshot of source before this script is run
# Usage: python3 set_binlog.py 'production-encrypted-aurora-cluster' 'production-encrypted-looker-replica-cluster'
# Sets up a binlog replication with data moving from 'production-encrypted-aurora-cluster' to 'production-encrypted-looker-replica-cluster'

import re
import sys
import time
import boto3
import sqlalchemy as sa

try:
    import pymysql

    pymysql.install_as_MySQLdb()
except ImportError:
    pass

us_west_1 = boto3.session.Session(region_name="us-west-1")
ssm = us_west_1.client("ssm")
rds = us_west_1.client("rds")
logs = boto3.client("logs")


def get_engine(user, passw, hostname):
    return sa.create_engine(
        f"mysql+mysqldb://{user}:{passw}@{hostname}/heart_db?charset=utf8mb4",
        pool_pre_ping=True,
        pool_recycle=1_800,
    )


def get_param(x):
    return ssm.get_parameter(Name=x, WithDecryption=True)["Parameter"]["Value"]


# Cluster to provide data in binlog
SOURCE_CLUSTER = sys.argv[1]
# Cluster to recieve data in binlog
DEST_CLUSTER = sys.argv[2]
# Binlog replica user credentials used to read from source
replica_user = "tech-replica"
replica_password = get_param(f"/Sensitive/Database/Credentials/{replica_user}")
# Admin user credentials used to set up binlog in destination
admin_user = "flywheel"
admin_password = get_param(f"/Sensitive/Database/Credentials/{admin_user}")

# Destination connectivity data
dest_instances = rds.describe_db_instances(
    Filters=[{"Name": "db-cluster-id", "Values": [DEST_CLUSTER]}]
)["DBInstances"]
dest_instance_id = dest_instances[0]["DBInstanceIdentifier"]
dest_instance_endpoint = dest_instances[0]["Endpoint"]["Address"]
dest_instance_port = dest_instances[0]["Endpoint"]["Port"]
dest_engine = get_engine(
    admin_user, admin_password, f"{dest_instance_endpoint}:{dest_instance_port}"
)
password = "SuperS3cr:tP!3#11"
# Source connectivity Info
source_cluster = [
    l
    for l in [
        x["DBClusterMembers"]
        for x in rds.describe_db_clusters()["DBClusters"]
        if x["DBClusterIdentifier"] == SOURCE_CLUSTER
    ][0]
    if l["IsClusterWriter"]
]
source_instance_id = source_cluster[0]["DBInstanceIdentifier"]
source_instances = rds.describe_db_instances(
    Filters=[{"Name": "db-instance-id", "Values": [source_instance_id]}]
)["DBInstances"]
source_instance_endpoint = source_instances[0]["Endpoint"]["Address"]
source_instance_port = source_instances[0]["Endpoint"]["Port"]


# Get the latest log stream TODO: make sure you get all the next tokens and order by creation time ascending to find oldest
log_group_name = f"/aws/rds/cluster/{DEST_CLUSTER}/error"
log_streams = logs.describe_log_streams(
    descending=False,
    logGroupName=log_group_name,
    # orderBy="LastEventTime",
    logStreamNamePrefix=dest_instance_id,
)

# Get the earliest events in log
events = logs.get_log_events(
    logGroupName=log_group_name,
    logStreamName=log_streams["logStreams"][0]["logStreamName"],
    startFromHead=True,
)
evs = events["events"] + []
next_token = None
while next_token != events["nextForwardToken"]:
    events = logs.get_log_events(
        logGroupName=log_group_name,
        logStreamName=log_streams["logStreams"][0]["logStreamName"],
        startFromHead=False,
        nextToken=events["nextForwardToken"],
    )
    if next_token != events["nextForwardToken"]:
        evs += events["events"]
        next_token = events["nextForwardToken"]

# Filter out the needed values from the message
binlog_regex = re.compile(
    r"\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d.\d+Z\s+\d\s+\[Note\]\s+InnoDB:\sLast\sMySQL\sbinlog\sfile\sposition\s\d+\s(\d+),\sfile\sname\s(mysql-bin-changelog.\d+)"
)
# Get oldest message that matches our regex
bin_messages = [
    (x["timestamp"], binlog_regex.search(x["message"]))
    for x in evs
    if binlog_regex.search(x["message"])
]
bin_messages.sort(key=lambda x: x[0])
latest_message = bin_messages[0][1]
position, filename = (latest_message.group(1), latest_message.group(2))

# Build query now that we have all the necessary info
set_up_query = f"CALL mysql.rds_set_external_master ('{source_instance_endpoint}', {source_instance_port}, '{replica_user}', '{replica_password}', '{filename}', {position}, 0);"

with dest_engine.connect() as conn:
    print(f"Executing on {dest_engine}...")
    print(set_up_query)
    input("Ready? (press any key to start replication)")
    # t = conn.execute(set_up_query)
    # print(f"Result: {t.fetchall()}")
    time.sleep(2)
    print("Starting binlog now that config is set...")
    t = conn.execute("CALL mysql.rds_start_replication;")
    print(f"Result: {t.fetchall()}")
    time.sleep(2)
    print("Showing Slave Status")
    while True:
        t = conn.execute("SHOW SLAVE STATUS;")
        print(f"{t.fetchall()}")
        input("Press any key to refresh slave status...")
