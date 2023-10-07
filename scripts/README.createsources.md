The `createsources.py` script bulk-creates sources and connections in a Dalgo workspace.

Prerequisites:
- a Dalgo workspace with a warehouse

Usage:
```
 PYTHONPATH=<LOCAL_DDP_BACKEND_DIR> python scripts/createsources.py \
  --env <ENV_FILE> \
  --file <sources-and-connections.yaml>
```

The `ENV_FILE` needs to contain the following variables

```
APP_HOST="staging-api.dalgo.in"
APP_PORT=443
EMAIL=<your dalgo login email>
PASSWORD=<your dalgo password>
ORG=<your org's slug>
```

Make sure to change the `APP-HOST` to `api.dalgo.in` if you are doing this in production. The `org slug` is the lowercased version of the Org name, with spaces replaced by `-`.

The configuration YAML has the following structure:

```
sources:
  - name: NAME
    stype: SOURCE TYPE
    config:
      parameter: value
      parameter: value
      parameter: value
```

You have to know the `SOURCE_TYPE` as well as the configuration structure for your source.

After the `sources` you will list the `connections` in the same YAML file:

```
connections:
  - name: NAME
    source: SOURCE_NAME_FROM_ABOVE
    destinationSchema: staging // or whatever schema you want
    streams:
      - name: NAME_OF_TABLE_IN_DESTINATION
        syncMode: overwrite // or append or whatever
```

Running the script should then create the sources in your Dalgo workspace.
