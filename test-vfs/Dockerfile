# cannot use a musl based image, as the following expects dynamically linked libs
FROM rust:1-slim

# add build dependencies
RUN apt-get update && apt-get install -y \
    curl build-essential tcl-dev valgrind unzip \
    && rm -rf /var/lib/apt/lists/*

# create non-root user to execute the tests (some tests fail if executed as root)
RUN addgroup -gid 1000 --system sqlite && \
    adduser -uid 1000 --system --gid 1000 --home /home/sqlite --shell /bin/bash sqlite && \
    passwd -d sqlite
WORKDIR /home/sqlite

# download and extract sqlite source
RUN curl -LO https://www.sqlite.org/2022/sqlite-src-3370200.zip && unzip sqlite-src-3370200.zip

# build a stub of the test vfs, which allows to build and cache sqlite before building the actual
# test vfs
ADD --chown=sqlite docker/test-vfs /home/sqlite/test-vfs
RUN cd test-vfs && cargo build

# build sqlite test programs
COPY patch/Makefile.in.patch Makefile.in.patch
RUN patch -u sqlite-src-3370200/Makefile.in Makefile.in.patch
RUN mkdir build && cd build && ../sqlite-src-3370200/configure
COPY patch/test_ext.c sqlite-src-3370200/src/test_ext.c
RUN cd build && make \
    OPTS="-DSQLITE_EXTRA_INIT=sqlite3_register_test_vfs" \
    LIBS="-L/home/sqlite/test-vfs/target/debug/ -ltest_vfs" \
    USE_AMALGAMATION=0 \
    testfixture
RUN chown -R sqlite:sqlite /home/sqlite/build
RUN rm -rf /home/sqlite/test-vfs

# Remove unix file system specific tests from wal2 that expect the db-shm to be persisted to disk
# and also care about file permissions for db-wal and db-shm files. Since the test VFS isn't meant
# for saving files to disk, those tests are removed.
COPY patch/test/wal2.test.patch wal2.test.patch
RUN patch sqlite-src-3370200/test/wal2.test wal2.test.patch && rm wal2.test.patch

RUN mkdir /home/sqlite/lib/
ENV LD_LIBRARY_PATH /home/sqlite/lib/

WORKDIR /github/workspace
COPY entrypoint.sh /
ENTRYPOINT ["/entrypoint.sh"]