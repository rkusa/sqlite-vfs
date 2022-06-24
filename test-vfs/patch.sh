#!/bin/bash
set -e

# The struct created per opened db file is bigger for the test-vfs compared to the original os_unix
# one, which is why some expected values in the following test need to be updated.
patch test/dbstatus.test ../patch/dbstatus.test.patch

# Remove e_walauto.test as it requires an actually memory mapped wal index
rm test/e_walauto.test
rm test/mmapwarm.test

# Remove external_reader.test as the unix-specific SQLITE_FCNTL_EXTERNAL_READER is not implemented.
rm test/external_reader.test

# Remove oserror.test as it tests specifics of the default unix/windows VFS modules.
rm test/oserror.test

# Well, I couldn't figure out how to build the needed `libtestloadext.so`, so they are skipped for
# now.
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

