#!/bin/bash
set -e

cd /github/workspace
cargo build -p test-vfs --no-default-features
cp target/debug/libtest_vfs.so /home/sqlite/lib/

cd /home/sqlite/build

# remove tests (to ignore them) related to WAL (which is not yet supported)
# rm ../sqlite-src-3370200/test/busy2.test
rm ../sqlite-src-3370200/test/chunksize.test

su \
  -c "./testfixture ../sqlite-src-3370200/$1 --verbose=file --output=test-out.txt" \
  sqlite
