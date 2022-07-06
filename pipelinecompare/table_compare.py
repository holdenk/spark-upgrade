import argparse
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

parser = argparse.ArgumentParser(
    description='Compare two different versions of a pipeline')
parser.add_argument('--tables', type=str, nargs='+', required=True,
                    help='Name of the tables.')
parser.add_argument('--format', type=str, help='Format of the table')
parser.add_argument('--control-root', type=str,
                    help='root directory for the control files')
parser.add_argument('--target-root', type=str,
                    help='root directory for the target files')
parser.add_argument('--control-tables', type=str,
                    nargs='+', help='control tables')
parser.add_argument('--target-tables', type=str,
                    nargs='+', help='target tables')
args = parser.parse_args()


def compare_tables(control, target):
    if control.schema != target.schema:
        control.printSchema()
        target.printSchema()
        raise Exception(
            f"Control schema {control.schema} and target schema {target.schema} do not match")
    control.persist()
    target.persist()
    control_count = control.count()
    target_count = target.count()
    # Do diffs on the data, but subtract doesn't support all data types so fall back to strings.
    # TODO: only convert the columns that need to be converted.
    try:
        missing_rows = control.subtract(target)
        new_rows = target.subtract(control)
    except Exception as e:
        print(f"Warning converting all to strings.... {e}")
        columns = control.columns
        for c in columns:
            control = control.withColumn(c, control[c].cast('string'))
            target = target.withColumn(c, target[c].cast('string'))
        missing_rows = control.subtract(target)
        new_rows = target.subtract(control)
    new_rows_count = new_rows.count()
    if new_rows_count > 0:
        print(f"Found {new_rows_count} that were not in the control")
        new_rows.show()
    missing_rows_count = missing_rows.count()
    if missing_rows_count > 0:
        print(f"Found {missing_rows_count} missing from new new pipeline")
        missing_rows.show()
    if new_rows_count > 0 or missing_rows_count > 0:
        raise Exception("Data differs in table, failing.")

    if control_count != target_count:
        print(f"Counts do not match! {control_count} {target_count}")
        try:
            # Handle duplicates, will fail on maps.
            counted_control = control.groupBy(
                *control.columns).count().persist()
            counted_target = target.groupBy(*target.columns).count().persist()
            new_rows = counted_target.subtract(counted_control)
            missing_rows = counted_control.subtract(counted_target)
            new_rows_count = new_rows.count()
            if new_rows_count > 0:
                print(f"Found {new_rows_count} that were not in the control")
                new_rows.show()
            missing_rows_count = missing_rows.count()
            if missing_rows_count > 0:
                print(
                    f"Found {missing_rows_count} missing from new new pipeline")
                missing_rows.show()
        except Exception as e:
            raise Exception(f"Data counts differ but {e} prevents grouping cmp")


if args.control_root is not None:
    for table in args.tables:
        control = spark.read.format(args.format).load(
            f"{args.control_root}/{table}")
        target = spark.read.format(args.format).load(
            f"{args.target_root}/{table}")
        compare_tables(control, target)
else:
    tables = zip(args.control_tables, args.target_tables)
    for (ctrl_name, target_name) in tables:
        compare_tables(spark.read.table(ctrl_name),
                       spark.read.table(target_name))
