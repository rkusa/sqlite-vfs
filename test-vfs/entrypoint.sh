#!/bin/bash
set -e

cd /github/workspace
cargo build -p test-vfs
cp target/debug/libtest_vfs.so /home/sqlite/lib/

# open the directory with the pre-build sqlite
cd /home/sqlite/build

su -c "time ./testfixture ../sqlite-src-3370200/$1 --verbose=false" \
  sqlite
