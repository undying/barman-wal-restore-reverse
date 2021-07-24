
# barman-wal-restore-reverse

If you use barman to restore the database, then it already speeds up the recovery process from WAL logs quite well by downloading them in parallel.

But this process can still be accelerated by downloading logs starting from the end, until the logs meet somewhere in the middle.

When all the logs are downloaded, PostgreSQL does not need to wait for their transmission over the network, so recovery is much faster.

## Usage

```sh
postgres@my-db:~$ barman-wal-restore-reverse.py \
  --server-name=my_production_db \
  --backup-id=20210724T041515
```

The script downloads WAL logs from the Barman server.
You need to run it from a user who has SSH access to the barman server and has access to the folder with WAL logs on the database server. As a rule, this is a postgres user.

