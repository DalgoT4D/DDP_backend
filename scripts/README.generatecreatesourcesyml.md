This is a convenience script to generate the YAML file for `createsources.py` specifically for CSV sources in S3

Usage:
```
python generatecreatesourcesyml.py \
  --csvs <CSV LISTING THE SOURCE CSVS> \
  --aws-access-key-id <AWS_ACCESS_KEY_ID> \
  --aws-secret-access-key <AWS_SECRET_ACCESS_KEY> \
  --output <YAML for createsources.py>
  --destination-schema <DESTINATION SCHEMA, defaults to "staging">
```

The AWS credentials are not used by this script, they are put into the YAML for use by Airbyte.

The input CSV must have the following columns:

`s3location,dataset,source,connection`

- `s3location` is an S3 url of the form `s3://...`
- `dataset` is the name of the destination table to write to
- `source` is the name of the Dalgo source to be created
- `connection` is the name of the Dalgo connection to be created

The `dataset`, `source` and `connection` will often all be the same.

Running this script will generate the YAML which can be passed to `createsources.py` to create the corresponding sources and their connections to the Dalgo warehouse.
