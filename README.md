# CassandraDatum
[![Build Status](https://travis-ci.org/backupify/exception_helper.svg)](https://travis-ci.org/backupify/exception_helper)
[![Coverage Status](https://coveralls.io/repos/backupify/exception_helper/badge.svg)](https://coveralls.io/r/backupify/exception_helper)
[![Code Climate](https://codeclimate.com/github/backupify/exception_helper/badges/gpa.svg)](https://codeclimate.com/github/backupify/exception_helper)

## Test setup

Do this in cassandra-cli:

```
use BackupifyMetadata_test; # create it first if it doesn't exist
create column family MockCassandraData with column_type='Super' and comparator='com.backupify.db.DatumType' and subcomparator='UTF8Type';
```

## Copyright

Copyright (c) 2012 Jason Haruska. See LICENSE.txt for further details.
