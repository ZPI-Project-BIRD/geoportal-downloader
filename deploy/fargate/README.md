# Fargate environment configuration

All three binaries use the same environment-variable contract. At startup they
load `.env` when present, but existing process variables always win. In ECS,
do not package `.env` in the image: inject configuration through the task
definition instead.

Use `environment.env.example` for non-secret settings. Upload a populated copy
to a private S3 bucket and attach it to each container with the ECS
`environmentFiles` property.

Use `secrets.json.example` as the task-definition `secrets` fragment. Replace
the placeholder ARNs with Secrets Manager ARNs and grant the task execution
role `secretsmanager:GetSecretValue` for those secrets. If the secret uses a
customer-managed KMS key, also grant `kms:Decrypt`.

The services require these secrets:

- `RABBITMQ_URL`: downloader API, download worker, processor worker
- `DATABASE_URL`: download worker
- `MINIO_ACCESS_KEY`: download worker, processor worker
- `MINIO_SECRET_KEY`: download worker, processor worker

The processor image must include `gdal2tiles.py` and its GDAL runtime. Set
`GDAL2TILES_COMMAND` if the executable is installed at another path.
