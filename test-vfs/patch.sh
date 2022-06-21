#!/bin/bash
set -e

# Remove unix file system specific tests from wal2 that expect the db-shm to be persisted to disk
# and also care about file permissions for db-wal, db-shm and journal files. Since the test VFS
# isn't meant for saving files to disk, those tests are removed.
patch test/wal2.test ../patch/wal2.test.patch
rm test/journal3.test

# Remove e_walauto.test as it requires an actually memory mapped wal index
rm test/e_walauto.test
rm test/mmapwarm.test

# Remove external_reader.test as the unix-specific SQLITE_FCNTL_EXTERNAL_READER is not implemented.
rm test/external_reader.test

# Remove oserror.test as it tests specifics of the default unix/windows VFS modules.
rm test/oserror.test

# Loading extensions is not supported
rm test/loadext.test
rm test/loadext2.test

# Remove long running tests that while being green, don't contribute tests relevant for a VFS
# implementation so it is not forth waiting for them.
rm test/backup_ioerr.test
rm test/speed1.test
rm test/speed1p.test
rm test/speed2.test
rm test/speed3.test
rm test/speed4.test
rm test/speed4p.test

# Remove tests that only test a specific built-in VFS
rm test/unixexcl.test

