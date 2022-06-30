import argparse
import sys
from utils import *
import uuid
import asyncio
import subprocess

parser = argparse.ArgumentParser(description='Compare two different versions of a pipeline')
parser.add_argument('--input-tables', type=str, nargs='*',
                    help='Name of the input tables (required for iceberg, optional for lakefs)')
parser.add_argument('--output-tables', type=str, nargs='+', required=True,
                    help='Name of the output tables.')
parser.add_argument('--repo', type=str, help='lakefs repo')
parser.add_argument('--src-branch', type=str, help='src branch to fork from',
                    default='main')
parser.add_argument('--iceberg', action='store_true',
                    help='Use iceberg to create snapshots for comparisons.')
parser.add_argument('--lakeFS', action='store_true',
                    help='Use lakeFS to create snapshots for comparisons.')
parser.add_argument('--spark-command', type=str, nargs='+', default=["spark-submit"],
                    help="Command to run the comparison pipeline.")
parser.add_argument('--spark-sql-command', type=str, nargs='+', default=["spark-sql"],
                    help="Command to run spark sql")
parser.add_argument('--format', type=str, help='Format of the output tables')
# Not yet implemented
#parser.add_argument('--raw', action='store_true',
#                    help='Just use raw HDFS (compatible) storage. Involves copying data.')
#parser.add_argument('--tmpdir', type=str,
#                    help='Temporary directory to use for comparisons.')
# TODO: Add tolerance :)
#parser.add_argument('--tolerance', type=float, default=0.001,
#                    help='Tolerance for float comparisons.')
parser.add_argument('--control-pipeline', type=str, required=True,
                    help='Control pipeline. Will be passed through the shell.' +
                    'Metavars are {branch_name}, {input_tables}, and {output_tables}')
parser.add_argument('--new-pipeline', type=str, required=True,
                    help='New pipeline. Will be passed through the shell.' +
                    'Metavars are {branch_name}, {input_tables}, and {output_tables}')
parser.add_argument('--no-cleanup', action='store_true')
args = parser.parse_args()

print(args)

async def run_pipeline(command, output_tables, input_tables=None, branch_name=None):
    """
    Async run the pipeline for given parameters. Returns a proc object for
    the caller to await communicate on.
    """
    import os
    if input_tables is not None:
        command.replace("{input_tables}", " , ".join(input_tables.join))
    if output_tables is not None:
        command.replace("{output_tables}", " , ".join(output_tables))
    if branch_name is not None:
        command.replace("{branch_name}", branch_name)
    return await asyncio.create_subprocess_exec(
        'bash','-c', command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE)


if args.lakeFS:
    print("Using lakefs")
    import lakefs_client
    from lakefs_client import models
    from lakefs_client.client import LakeFSClient
    import yaml
    import os
    # TODO: Match real config instead of whatever I came up with.
    # Or update lakefs client to read lakectlyaml file?
    conf_file = open(os.path.expanduser("~/.lakectl.yaml"), "r")
    conf = yaml.safe_load(conf_file)
    config = lakefs_client.Configuration()
    config.username = conf['username']
    config.password = conf['password']
    config.host = conf['host']
    client = LakeFSClient(config)
    branch_prefix = f"magic-cmp-{uuid.uuid1()}"
    branch_names = [f"{branch_prefix}",
                    f"{branch_prefix}_control",
                    f"{branch_prefix}_test"]
    try:
        # Create an initial branch which we can then fork control and test from
        # This avoids a race if we forked both from main.
        client.branches.create_branch(
            repository=args.repo,
            branch_creation=models.BranchCreation(name=branch_prefix, source=args.src_branch))
        client.branches.create_branch(
            repository=args.repo,
            branch_creation=models.BranchCreation(name=branch_names[1], source=branch_prefix))
        client.branches.create_branch(
            repository=args.repo,
            branch_creation=models.BranchCreation(name=branch_names[2], source=branch_prefix))
        # Run the pipelines concurrently.
        async def run_pipelines():
            ctrl_pipeline_proc = await run_pipeline(args.control_pipeline, args.output_tables, branch_name=branch_names[1])
            new_pipeline_proc = await run_pipeline(args.new_pipeline, args.output_tables, branch_name=branch_names[2])
            cstdout, cstderr = await ctrl_pipeline_proc.communicate()
            nstdout, nstderr = await new_pipeline_proc.communicate()
            if ctrl_pipeline_proc.returncode != 0:
                print("Error running contorl pipeline")
                print(cstdout.decode())
                print(cstderr.decode())
            if new_pipeline_proc.returncode != 0:
                print("Error running new pipeline")
                print(nstdout.decode())
                print(nstderr.decode())
            if ctrl_pipeline_proc.returncode != 0 or new_pipeline_proc.returncode != 0:
                raise Exception("Error running pipelines.")
        asyncio.run(run_pipelines())
        # Compare the outputs
        cmd = args.spark_command
        cmd.extend([
            "--driver-memory", "10G",
            "--conf", f"spark.hadoop.fs.s3a.access.key={conf['username']}",
            "--conf", f"spark.hadoop.fs.s3a.secret.key={conf['password']}",
            "--conf", f"spark.hadoop.fs.s3a.endpoint={conf['host']}",
            "--conf", "spark.hadoop.fs.s3a.path.style.access=true",
            "--conf", "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            "--conf", "spark.jars.packages=org.apache.hadoop:hadoop-aws:3.2.2,com.amazonaws:aws-java-sdk-bundle:1.11.563",
            "table_compare.py",
            "--format", args.format,
            "--control-root", f"s3a://{args.repo}/{branch_names[1]}",
            "--target-root", f"s3a://{args.repo}/{branch_names[2]}",
            "--tables"])
        cmd.extend(args.output_tables)
        subprocess.run(cmd)
    finally:
        # Cleanup the branches
        if not args.no_cleanup:
            for branch_name in branch_names:
                try:
                    client.branches.delete_branch(
                        repository=args.repo, branch=branch_name)
                except:
                    print(f"Skipping deleting branch {branch_name}")
elif args.iceberg:
    print("Using iceberg.")
    # See discussion in https://github.com/apache/iceberg/issues/2481
    # currently no git like branching buuuut we can hack something "close enough"
    magic = f"magic-cmp-{uuid.uuid1()}"
    tbl_id = 0
    def snapshot_ish(table_name):
        cmd = args.spark_sql_command
        cmd.extend(["-e", f"SELECT snapshot_id FROM  {table_name}.history WHERE is_current_ancestor == true AND parent_id == NULL"], capture_output=True)
        proc = subprocess.run(cmd)
        currentSnapshot = proc.stdout
        snapshot_name = f"{table_name}@{tbl.currentSnapshot}"
        print(snapshot_name)
        return snapshot_name

    def make_tbl_like(table_name):
        global tbl_id
        tbl_id = tbl_id + 1
        new_table_name = f"{tbl_id}{magic}"
        cmd = args.spark_sql_command
        cmd.extend(["-e", f"CREATE TABLE {new_table_name}  LIKE {table_name}"])
        subprocess.run(cmd)
        return new_table_name

    snapshotted_tables = list(map(snapshot_ish, args.input_tables))
    try:
        ctrl_output_tables = list(map(make_tbl_like, args.output_tables))
        new_output_tables = list(map(make_tbl_like, args.output_tables))
        # Run the pipelines concurrently
        async def run_pipelines():
            ctrl_pipeline_proc = await run_pipeline(args.control_pipeline, ctrl_output_tables, input_tables=snapshotted_tables)
            new_pipeline_proc = await run_pipeline(args.new_pipeline, new_output_tables, input_tables=snapshotted_tables)
            cstdout, cstderr = await ctrl_pipeline_proc.communicate()
            nstdout, nstderr = await new_pipeline_proc.communicate()
            if ctrl_pipeline_proc.returncode != 0:
                print("Error running contorl pipeline")
                print(cstdout.decode())
                print(cstderr.decode())
            if new_pipeline_proc.returncode != 0:
                print("Error running new pipeline")
                print(nstdout.decode())
                print(nstderr.decode())
            if ctrl_pipeline_proc.returncode != 0 or new_pipeline_proc.returncode != 0:
                raise Exception("Error running pipelines.")
        asyncio.run(run_pipelines())
        # Compare the outputs
        cmd = args.spark_command
        cmd.extend([
            "--class", "com.holdenkarau.tblcmp.Compare",
            "table_compare.py",
            "--tolerance", f"{args.tolerance}"
            "--control-tables"])
        cmd.extend(ctrl_output_tables)
        cmd.extend(["--new-tables"])
        cmd.extend(new_output_tables)
        subprocess.run(cmd)
    finally:
        if not args.no_cleanup:
            for tid in range(0, tbl_id):
                table_name = f"{tid}{magic}"
                cmd = args.spark_sql_command
                cmd.extend(["-e", f"DROP TABLE {table_name}"])
                subprocess.run(cmd)

else:
    eprint("You must chose one of iceberg or lakefs for input tables.")
    sys.exit(1)
