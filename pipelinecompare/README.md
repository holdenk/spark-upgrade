# Getting started

Install requirements from `requirements.txt`, create two different pipelines, build the parent table comparision project.

# Samples

## Iceberg sample

## LakeFS Sample
- sign up for lakefs demo
- create a ~/.lakectl.yaml file with `username` `password` and `host`.
- run following command (compares two no-op pipelines on exiting output, should succeed).

`python domagic.py  --control-pipeline "ls /" --input-tables farts mcgee --lakeFS --repo my-repo --new-pipeline "ls /" --output-tables sample_data`

### LakeFS FAQ

Why don't you just use commit hashes?

Many things can result in different binary data on disk but still have the same effective data stored.
