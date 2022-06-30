# Getting started

Install requirements from `requirements.txt`, create two different pipelines, build the parent table comparision project.

# Open questions

Is shelling through the command line the right approach? Benefit: we don't need to run inside of spark-submit.
Do we want to support "raw" tables?


# Samples

## Iceberg sample

## LakeFS Sample
- sign up for lakefs demo
- create a ~/.lakectl.yaml file with `username` `password` and `host`.
- run following command (compares two no-op pipelines on exiting output, should succeed).

`python domagic.py  --control-pipeline "ls /" --input-tables farts mcgee --lakeFS --repo my-repo --new-pipeline "ls /" --output-tables sample_data`

OR if your running in local mode:

`python domagic.py  --control-pipeline "ls /" --input-tables farts mcgee --lakeFS --repo my-repo --new-pipeline "ls /" --output-tables "sample_data/release=v1.9/type=relation/20220106_182445_00068_pa8u7_04924a3b-01b0-4174-9772-7285db53a68c" --format parquet`

### LakeFS FAQ

Why don't you just use commit hashes?

Many things can result in different binary data on disk but still have the same effective data stored.
