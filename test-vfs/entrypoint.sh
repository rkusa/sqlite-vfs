#!/bin/bash
set -e

cd /github/workspace
cargo build -p test-vfs --no-default-features
cp target/debug/libtest_vfs.so /home/sqlite/lib/

cargo build -p durable-object --bin server

# open the directory with the pre-build sqlite
cd /home/sqlite/build

# run storage server
su -c "mkdir -p testdir" sqlite
cd testdir # run in testdir as the database files must be created in there
su -c "/github/workspace/target/debug/server" sqlite &
sleep 1 # give server time to start up
cd ..

su -c "./testfixture ../sqlite-src-3370200/$1 --verbose=file --output=test-out.txt" \
  sqlite
