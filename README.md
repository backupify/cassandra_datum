# CassandraDatum
[![Build Status](https://travis-ci.org/backupify/exception_helper.svg)](https://travis-ci.org/backupify/exception_helper)
[![Coverage Status](https://coveralls.io/repos/backupify/exception_helper/badge.svg)](https://coveralls.io/r/backupify/exception_helper)
[![Code Climate](https://codeclimate.com/github/backupify/exception_helper/badges/gpa.svg)](https://codeclimate.com/github/backupify/exception_helper)

## Test setup

Updated for environments using a dockerized cassandra version:
Connect to docker instance and start cqlsh to create the test keyspace:
```
docker exec -i -t cass1 sh -c 'exec cqlsh 127.0.0.1'
```
from cqlsh:
```
CREATE KEYSPACE IF NOT EXISTS BackupifyMetadata_test WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor' : 3 };
```

Then these old commands should work from cassandra-cli, but they should be updated to work from cqlsh as we upgrade cass-
to get to cassandra-cli, `exit` from cqlsh and run:
```
docker exec -i -t cass1 sh -c 'exec cassandra-cli'
```
Then use the old commands to use the keyspace and create the column family for tests

```
use BackupifyMetadata_test;
create column family MockCassandraData with column_type='Super' and comparator='com.backupify.db.DatumType' and subcomparator='UTF8Type';
```

## Copyright

Copyright (c) 2012 Jason Haruska. See LICENSE.txt for further details.
