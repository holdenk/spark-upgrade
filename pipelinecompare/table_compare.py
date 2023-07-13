import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import FractionalType
from utils import *
from spark_utils import *

spark = SparkSession.builder.getOrCreate()

parser = argparse.ArgumentParser(
    description='Compare two different versions of a pipeline.' +
    'Either --tables and --control-root and --target-root or ' +
    '--control-tables and --target-tables must be specified.')
parser.add_argument('--tables', type=str, nargs='+', required=False,
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
parser.add_argument('--compare-precision', type=int,
                    help='Precision for fractional comparisons.')
parser.add_argument('--row-diff-tolerance', type=float, default=0.0,
                    help='Tolerance for % of different rows')


def compare_tables(control, target):
    if control.schema != target.schema:
        control.printSchema()
        target.printSchema()
        raise Exception("Control schema and target schema do not match")
    if args.compare_precision is not None:
        columns = control.columns
        schema = control.schema
        for c in control.columns:
            if isinstance(schema[c].dataType, FractionalType):
                control = control.withColumn(
                    c, round(control[c], parser.compare_precision))
                target = control.withColumn(
                    c, round(target[c], parser.compare_precision))
    control.persist()
    target.persist()
    control_count = control.count()
    target_count = target.count()
    # Do diffs on the data, but subtract doesn't support all data types so fall back to strings.
    try:
        missing_rows = control.subtract(target)
        new_rows = target.subtract(control)
    except Exception as e:
        # TODO: only convert the columns that need to be converted.
        eprint(f"Warning converting all to strings.... {e}")
        columns = control.columns
        for c in columns:
            control = control.withColumn(c, control[c].cast('string'))
            target = target.withColumn(c, target[c].cast('string'))
        missing_rows = control.subtract(target)
        new_rows = target.subtract(control)
    new_rows.cache()
    missing_rows.cache()
    new_rows_count = new_rows.count()
    if new_rows_count > 0:
        eprint(f"Found {new_rows_count} that were not in the control")
        new_rows.show()
    missing_rows_count = missing_rows.count()
    if missing_rows_count > 0:
        eprint(f"Found {missing_rows_count} missing from new new pipeline")
        missing_rows.show()
    changed_rows = new_rows_count + missing_rows_count
    row_diff_tol = args.row_diff_tolerance
    exact_tol = row_diff_tol * control_count
    if changed_rows > exact_tol:
        error(
            f"Data differs in table by more than {100 * row_diff_tol}%, failing.")
    else:
        eprint(f"Different {changed_rows} within {row_diff_tol}% ({exact_tol})")

    if control_count != target_count:
        eprint(f"Counts do not match! {control_count} {target_count}")
        try:
            # Handle duplicates, will fail on maps.
            counted_control = control.groupBy(
                *control.columns).count().persist()
            control_count = counted_control.count()
            counted_target = target.groupBy(*target.columns).count().persist()
            new_rows = counted_target.subtract(counted_control)
            missing_rows = counted_control.subtract(counted_target)
            new_rows_count = new_rows.count()
            if new_rows_count > 0:
                print(f"Found {new_rows_count} grouped that were not in the control")
                new_rows.show()
            missing_rows_count = missing_rows.count()
            if missing_rows_count > 0:
                print(
                    f"Found {missing_rows_count} grouped missing from new new pipeline")
                missing_rows.show()
            exact_tol = row_diff_tol * control_count
            changed_rows = new_rows_count + missing_rows_count
            if changed_rows > exact_tol:
                error(
                    f"Grouped data differs in table by more than {100 * row_diff_tol}%, failing.")
            else:
                print(f"Grouped different {changed_rows} within {row_diff_tol}% ({exact_tol})")
        except Exception as e:
            error(
                f"Data counts differ! And we ran into: {e}")


def run_comparisions(tables):
    for (ctrl_name, target_name) in tables:
        # Handle snapshots
        if "@" in ctrl_name:
            c_diff_view = None
            t_diff_view = None
            try:
                (c_diff_view, t_diff_view) = get_cdc_views(spark, ctrl_name, target_name)
            except Exception as e:
                # Otherwise fall through. Technically we could make CDC view from t0
                # But we are using the CDC view as just a hack for speed so not worth it.
                eprint("Falling through to legacy compare")
                (ctrl_name, c_snapshot) = ctrl_name.split("@")
                (target_name, t_snapshot) = target_name.split("@")
                return compare_tables(
                    spark.read.option("snapshot-id", c_snapshot).table(ctrl_name),
                    spark.read.option("snapshot-id", t_snapshot).table(target_name))
            print(f"Using CDC view to compare {c_diff_view} and {t_diff_view}")
            return compare_tables(
                c_diff_view,
                t_diff_view)
        else:
            compare_tables(
                spark.read.table(ctrl_name),
                spark.read.table(target_name))


if __name__ == "__main__":
    args = parser.parse_args()
    if args.control_root is not None:
        for table in args.tables:
            control = spark.read.format(args.format).load(
                f"{args.control_root}/{table}")
            target = spark.read.format(args.format).load(
                f"{args.target_root}/{table}")
            compare_tables(control, target)
    else:
        tables = zip(args.control_tables, args.target_tables)
        run_comparisions(tables)

    print("Table compare status: ok")
