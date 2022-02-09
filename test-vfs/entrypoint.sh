#!/bin/bash
set -e

cd /github/workspace
cargo build -p test-vfs --no-default-features
cp target/debug/libtest_vfs.so /home/sqlite/lib/

cd /home/sqlite/build
# s/extraquick.test/all.test/
su \
  -c "./testfixture ../sqlite-src-3370200/test/extraquick.test --verbose=file --output=test-out.txt" \
  sqlite
