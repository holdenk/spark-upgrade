import argparse
import sys
from utils import eprint
import uuid
import asyncio
import subprocess
import re
import os
import time

parser = argparse.ArgumentParser(
    description='Compare two different versions of a pipeline')
parser.add_argument('--input-tables', type=str, nargs='*',
                    help='Name of the input tables (required for legacy iceberg, ' +
                    'optional for lakefs and new iceberg)')
parser.add_argument('--output-tables', type=str, nargs='+', required=False,
                    help='Name of the output tables. Required for LakeFS and Legacy Iceberg.' +
                    'For new Iceberg will be used to filter the "relevant" tables.')
parser.add_argument('--repo', type=str, help='lakefs repo')
parser.add_argument('--src-branch', type=str, help='src branch to fork from',
                    default='main')
parser.add_argument('--iceberg-legacy', action='store_true',
                    help='Use iceberg to create snapshots for comparisons in seperate tables')
parser.add_argument('--iceberg', action='store_true',
                    help='Use iceberg to create snapshots using WAP pattern. ' +
                    'Must run in client mode. Uses iceberg-spark-upgrade-wap-plugin to' +
                    'add a listener.')
parser.add_argument('--lakeFS', action='store_true',
                    help='Use lakeFS to create snapshots for comparisons.')
parser.add_argument('--spark-command', type=str, nargs='+', default=["spark-submit"],
                    help="Spark command to run the comparison pipeline.")
parser.add_argument('--spark-sql-command', type=str, nargs='+', default=["spark-sql"],
                    help="Command to run spark sql")
parser.add_argument('--format', type=str, help='Format of the output tables')
parser.add_argument("--table-prefix", type=str, help="Prefix for temp tables.")
parser.add_argument('--compare-precision', type=int,
                    help='Precision for float comparisons.')
parser.add_argument('--row-diff-tolerance', type=float, default=0.0,
                    help='Tolerance for % of different rows')
# You can either fully specify the two different commands (these two args)
parser.add_argument('--control-pipeline', type=str, required=False,
                    help='Control pipeline. Will be passed through the shell.' +
                    'Metavars are {branch_name}, {spark_extra_conf}, {input_tables}, ' +
                    'and {output_tables}')
parser.add_argument('--new-pipeline', type=str, required=False,
                    help='New pipeline. Will be passed through the shell.' +
                    'Metavars are {branch_name}, {input_tables}, {output_tables}' +
                    'and {spark_extra_conf}')
# Special warehouse config
parser.add_argument('--warehouse-config', type=str, required=False,
                    help='Config passed to spark submit table compare')
# Or you can specify one pipeline and some parameters we'll generate new vs control.
parser.add_argument('--spark-control-command', type=str, required=False, default="spark-submit",
                    help="Spark command to run control pipeline")
parser.add_argument('--spark-new-command', type=str, required=False, default="spark-submit",
                    help="Spark command to run control pipeline")
parser.add_argument('--new-jar-suffix', type=str, nargs='?', default="",
                    help="Suffix to add to the new jar so we can tell different version apart.")
parser.add_argument('--combined-pipeline', type=str, required=False,
                    help='New pipeline. Will be passed through the shell.' +
                    'Metavars are {branch_name}, {input_tables}, {output_tables}' +
                    'and {spark_extra_conf}')
parser.add_argument('--no-cleanup', action='store_true')
args = parser.parse_args()

# Generate our pipelines
parsed_control_pipeline = args.control_pipeline
parsed_new_pipeline = args.new_pipeline

if ((args.control_pipeline is not None or args.new_pipeline is not None) and
    args.combined_pipeline is not None):

    print("You specified both control new and combined. Please choose one of two ways of" +
          "sepcifying yourpipeline.")
elif args.combined_pipeline is not None:
    combined_pipeline = args.combined_pipeline

    if "{spark_extra_conf}" not in combined_pipeline:
        # Find a place to insert our conf
        def insert_extra_conf(match):
            return "{spark_extra_conf} " + match.group(0)
        combined_pipeline = re.sub(
            "(--jar|--deploy-mode|--conf|--packages|--files|--py-files|" +
            "[\\-_\\w]+\\.jar|--class)",
            insert_extra_conf,
            combined_pipeline,
            1)
    parsed_control_pipeline = f"{args.spark_control_command} {combined_pipeline}"
    updated_combined_pipeline = combined_pipeline
    if args.new_jar_suffix is not None:
        def rewrite_jar(match):
            # group 0 is everything
            # group 1 is the seperator
            sep = match.group(1)
            # group 2 is the artifact "name"
            name = match.group(2)
            # group 3 is the artifact version (including the scala parts)
            v = match.group(3)
            return sep + name + args.new_jar_suffix + v + ".jar"
        updated_combined_pipeline = re.sub(
            "([ ,])([\\-_\\w/]+?)([\\-_.0-9]+)\\.jar",
            rewrite_jar,
            combined_pipeline)
    parsed_new_pipeline = f"{args.spark_new_command} {updated_combined_pipeline}"

print(args)

# Setup our commands for side-by-side comparison.

spark_sql_command = list(
    map(lambda x: x.replace("\\-", "-"), args.spark_sql_command))
spark_command = list(map(lambda x: x.replace("\\-", "-"), args.spark_command))


async def run_pipeline(command, output_tables=None, input_tables=None, branch_name=None,
                       spark_extra_conf=None):
    """
    Async run the pipeline for given parameters. Returns a proc object for
    the caller to await communicate on.
    """
    if input_tables is not None:
        command = command.replace("{input_tables}", " , ".join(input_tables))
    if output_tables is not None:
        command = command.replace("{output_tables}", " , ".join(output_tables))
    if branch_name is not None:
        if "{branch_name}" not in command:
            print("Could not find metavar {branch_name} to configure in " + command)
        command = command.replace("{branch_name}", branch_name)
    if spark_extra_conf is not None:
        if "{spark_extra_conf}" not in command:
            print("Could not find metavar {spark_extra_conf} to configure in " + command)
        command = command.replace("{spark_extra_conf}", spark_extra_conf)
    print(f"Running {command}")
    return await asyncio.create_subprocess_exec(
        'bash', '-c', command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE)


mytestid = str(uuid.uuid1()).replace("-", "_")
mytestid = mytestid.replace(";", "")
if args.lakeFS:
    print("Using lakefs")
    import lakefs_client
    from lakefs_client import models
    from lakefs_client.client import LakeFSClient
    import yaml
    # TODO: Match real config instead of whatever I came up with.
    # Or update lakefs client to read lakectlyaml file?
    conf_file = open(os.path.expanduser("~/.lakectl.yaml"), "r")
    conf = yaml.safe_load(conf_file)
    config = lakefs_client.Configuration()
    config.username = conf['username']
    config.password = conf['password']
    config.host = conf['host']
    client = LakeFSClient(config)
    branch_prefix = f"magic_cmp_{mytestid}"
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
            ctrl_pipeline_proc = await run_pipeline(
                parsed_control_pipeline, args.output_tables, branch_name=branch_names[1])
            new_pipeline_proc = await run_pipeline(
                parsed_new_pipeline, args.output_tables, branch_name=branch_names[2])
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
        # Commit the outputs
        try:
            client.commits.commit(
                repository=args.repo,
                branch=branch_names[1],
                commit_creation=models.CommitCreation(
                    message='Test data (control)',
                    metadata={'using': 'python_api'}))
        except Exception as e:
            eprint(
                f"Exception during commit {e}. This is expected for no-op pipelines.")
        try:
            client.commits.commit(
                repository=args.repo,
                branch=branch_names[2],
                commit_creation=models.CommitCreation(
                    message='Test data (new pipeline)',
                    metadata={'using': 'python_api'}))
        except Exception as e:
            eprint(
                f"Exception during commit {e}. This is expected for no-op pipelines.")
        # Compare the outputs
        # Note: we don't use lakeFS diff because the binary files can be different for a good number
        # of reasons, but underlying data is effectively the same (compression changes, etc.)
        # Possible future optimization: do lakeFS diff and short circuit if it is equal.
        cmd = spark_command.copy()
        cmd.extend([
            "--driver-memory", "10G",
            "--conf", f"spark.hadoop.fs.s3a.access.key={conf['username']}",
            "--conf", f"spark.hadoop.fs.s3a.secret.key={conf['password']}",
            "--conf", f"spark.hadoop.fs.s3a.endpoint={conf['host']}",
            "--conf", "spark.hadoop.fs.s3a.path.style.access=true",
            "--conf", "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",  # noqa
            "--conf", "spark.jars.packages=org.apache.hadoop:hadoop-aws:3.2.2,com.amazonaws:aws-java-sdk-bundle:1.11.563",   # noqa
            "table_compare.py",
            "--format", args.format,
            "--control-root", f"s3a://{args.repo}/{branch_names[1]}",
            "--target-root", f"s3a://{args.repo}/{branch_names[2]}",
            "--tables"])
        cmd.extend(args.output_tables)
        if args.row_diff_tolerance is not None:
            cmd.extend(["--row-diff-tolerance",
                        f"{args.row_diff_tolerance}"])
        if args.compare_precision is not None:
            cmd.extend(["--compare-precision",
                        f"{args.compare_precision}"])
        subprocess.run(cmd, check=True)
    finally:
        # Cleanup the branches
        if not args.no_cleanup:
            for branch_name in branch_names:
                try:
                    client.branches.delete_branch(
                        repository=args.repo, branch=branch_name)
                except Exception as e:
                    print(f"Skipping deleting branch {branch_name} due to {e}")
                    raise e
elif args.iceberg_legacy:
    print("Using iceberg in legacy mode")
    # See discussion in https://github.com/apache/iceberg/issues/2481
    # currently no git like branching buuuut we can hack something "close enough"
    magic = f"magic_cmp_{mytestid}"
    tbl_id = 0
    tbl_prefix = args.table_prefix

    def run_spark_sql_query(query):
        cmd = spark_sql_command.copy()
        cmd.extend(["-e"])
        cmd.extend([query])
        cmd_str = " ".join(cmd)
        print(f"Running {cmd_str}")
        proc = subprocess.run(cmd, capture_output=True)
        if proc.returncode != 0:
            raise Exception(
                f"Exception running {cmd} got stdout {proc.stdout} and stderr {proc.stderr}")
        return proc

    def snapshot_ish(table_name):
        proc = run_spark_sql_query(f"SELECT snapshot_id FROM  {table_name}.history WHERE is_current_ancestor == true AND parent_id IS NULL")  # noqa
        currentSnapshot = proc.stdout.decode("utf8")
        snapshot_name = f"{table_name}@{currentSnapshot}"
        print(f"Using snapshoted table {snapshot_name}")
        return snapshot_name

    def gen_table_name(tid):
        return f"{tbl_prefix}{tid}{magic}"

    def make_table_like(table_name):
        global tbl_id
        new_table_name = gen_table_name(tbl_id)
        run_spark_sql_query(
            f"CREATE TABLE {new_table_name}  LIKE {table_name}")
        tbl_id = tbl_id + 1
        return new_table_name

    snapshotted_tables = list(map(snapshot_ish, args.input_tables))
    try:
        ctrl_output_tables = list(map(make_table_like, args.output_tables))
        new_output_tables = list(map(make_table_like, args.output_tables))

        # Run the pipelines concurrently
        async def run_pipelines():
            new_pipeline_proc = await run_pipeline(
                parsed_new_pipeline, new_output_tables, input_tables=snapshotted_tables)
            # Bit of a hack, but incase one of them is going to make a new table we space them out.
            # Generally iceberg handles conflicts but at the create table time it's
            # less predictable in old versions.
            time.sleep(15)
            ctrl_pipeline_proc = await run_pipeline(
                parsed_control_pipeline, ctrl_output_tables, input_tables=snapshotted_tables)
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
        cmd = spark_command.copy()
        cmd.extend([
            "table_compare.py",
            "--control-tables"])
        cmd.extend(ctrl_output_tables)
        cmd.extend(["--target-tables"])
        cmd.extend(new_output_tables)
        if args.row_diff_tolerance is not None:
            cmd.extend(["--row-diff-tolerance",
                        f"{args.row_diff_tolerance}"])
        if args.compare_precision is not None:
            cmd.extend(["--compare-precision",
                        f"{args.compare_precision}"])
        subprocess.run(cmd, check=True)
    finally:
        print(f"Done comparing, cleaning up {tbl_id} tables.")
        if not args.no_cleanup:
            for tid in range(0, tbl_id):
                table_name = gen_table_name(tid)
                proc = run_spark_sql_query(f"DROP TABLE {table_name}")
elif args.iceberg:
    print("Using iceberg in WAP mode")
    # See discussion in https://github.com/apache/iceberg/issues/2481
    # https://github.com/apache/iceberg/issues/744
    # https://github.com/apache/iceberg/pull/342
    import re
    r = re.compile(
        r"""^IcebergListener: Created snapshot (\d+) on table (.+?) summary .+""",
        re.MULTILINE
    )

    try:
        table = []

        # TODO: Table extraction using the regex from WAPIcebergSpec
        # We dynamically create the control and target tables
        # If output_tables is specified in the params filter on it.
        ctrl_output_tables = []
        new_output_tables = []

        async def run_pipelines():
            script_path = os.path.realpath(os.path.dirname(__file__))
            plugin_target_path = (f"{script_path}/../iceberg-spark-upgrade-wap-plugin" +
                                  "/target/scala-2.12/")
            java_agent_path = (plugin_target_path +
                               "iceberg-spark-upgrade-wap-plugin_2.12-0.1.0-SNAPSHOT.jar")
            spark_extra_conf = f"--driver-java-options \"-javaagent:{java_agent_path}\""
            ctrl_pipeline_proc = await run_pipeline(
                parsed_control_pipeline, args.output_tables,
                spark_extra_conf=spark_extra_conf)
            new_pipeline_proc = await run_pipeline(
                parsed_new_pipeline, args.output_tables,
                spark_extra_conf=spark_extra_conf)
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
            new_match_itr = re.finditer(r, nstderr.decode())
            ctrl_match_itr = re.finditer(r, cstderr.decode())

            def match_to_table(m):
                return m.group(2) + "@" + m.group(1)

            print(nstderr.decode())
            new_output_tables = list(map(match_to_table, new_match_itr))
            ctrl_output_tables = list(map(match_to_table, ctrl_match_itr))
            return (ctrl_output_tables, new_output_tables)
        (ctrl_output_tables, new_output_tables) = asyncio.run(run_pipelines())
        print(f"Huzzah! ctrl: {ctrl_output_tables} new: {new_output_tables} :D")
        # Compare the outputs
        cmd = spark_command.copy()
        if args.warehouse_config is not None:
            warehouse_config = re.split("[\s+=]", args.warehouse_config)
            cmd.extend(warehouse_config)
        cmd.extend([
            "table_compare.py",
            "--control-tables"])
        cmd.extend(ctrl_output_tables)
        cmd.extend(["--target-tables"])
        cmd.extend(new_output_tables)
        if args.row_diff_tolerance is not None:
            cmd.extend(["--row-diff-tolerance",
                        f"{args.row_diff_tolerance}"])
        if args.compare_precision is not None:
            cmd.extend(["--compare-precision",
                        f"{args.compare_precision}"])
        cmd = list(filter(lambda x: x != "", cmd))
        subprocess.run(cmd, check=True)
    finally:
        print("Done!. You may want to cleanup snapshots.")

else:
    eprint("You must chose one of iceberg or lakefs for input tables.")
    sys.exit(1)
