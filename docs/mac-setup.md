## Setup minio (object store for the datalake)

```
brew install minio
brew services start minio
```

minio should be up and running at `http://127.0.0.1:49415/`
The default credentials are:
```
Username: minioadmin
Password: minioadmin
```

Create a bucket that will the base path for the iceberg datalake.

## Setup nessie (iceberg rest catalog)

```
docker pull ghcr.io/projectnessie/nessie

docker run -p 19120:19120 \
        -e NESSIE_CATALOG_DEFAULT-WAREHOUSE=telemetrylake \
        -e NESSIE_CATALOG_WAREHOUSES_TELEMETRYLAKE_LOCATION=s3://telemetrylake \
        -e NESSIE_CATALOG_SERVICE_S3_DEFAULT-OPTIONS_REGION=us-east-1 \
        -e NESSIE_CATALOG_SERVICE_S3_DEFAULT-OPTIONS_ENDPOINT=http://host.docker.internal:9000 \
        -e NESSIE_CATALOG_SERVICE_S3_DEFAULT-OPTIONS_PATH-STYLE_ACCESS=true \
        -e NESSIE_CATALOG_SERVICE_S3_DEFAULT-OPTIONS_AUTH-TYPE=STATIC \
        -e NESSIE_CATALOG_SERVICE_S3_DEFAULT-OPTIONS_ACCESS-KEY=urn:nessie-secret:quarkus:my-secrets.s3-default \
        -e MY-SECRETS_S3-DEFAULT_NAME=minioadmin \
        -e MY-SECRETS_S3-DEFAULT_SECRET=minioadmin \
        ghcr.io/projectnessie/nessie
```

Configuring `nessie.catalog.default-warehouse` and `nessie.catalog.warehouses.<warehouse-name>.location` are mandatory.
See [nessie configuration reference](https://projectnessie.org/nessie-latest/configuration/) for more details.

## Setup duckdb

```
brew install duckdb
```

Run `duckdb` and run the following in the sql shell:
```
D INSTALL iceberg;
D LOAD iceberg;
D INSTALL httpfs;
D LOAD httpfs;

D CREATE SECRET minio_secret (
    TYPE S3,
    KEY_ID 'minioadmin',
    SECRET 'minioadmin',
    REGION 'us-east-1',
    ENDPOINT '127.0.0.1:49415',
    USE_SSL false,
    URL_STYLE 'path'
);
```

Next attach to the nessie catalog named telemetrylake that was created from the `Setup nessie` step.

```
D ATTACH 'telemetrylake' AS telemetrylake (
      TYPE ICEBERG,
      ENDPOINT 'http://0.0.0.0:19120/iceberg/',
      AUTHORIZATION_TYPE 'NONE'
  );
```

Create the schema and table in duckdb:

```
D CREATE SCHEMA telemetrylake.main;

D CREATE TABLE telemetrylake.main.hello (
        id INTEGER,
        name VARCHAR
    );
```
