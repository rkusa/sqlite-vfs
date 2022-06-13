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
# -DSQLITE_EXTRA_INIT=sqlite3_register_test_vfs: custom init method that initializes the custom
#  test vfs
# -DSQLITE_MAX_MMAP_SIZE=0: disable memory mapping as it is not supported for the custom vfs
# -DSQLITE_DISABLE_DIRSYNC: disable to skip some tests that are not relevant for a custom vfs
RUN cd build && make \
    OPTS="-DSQLITE_EXTRA_INIT=sqlite3_register_test_vfs -DSQLITE_MAX_MMAP_SIZE=0 -DSQLITE_DISABLE_DIRSYNC" \
    LIBS="-L/home/sqlite/test-vfs/target/debug/ -ltest_vfs" \
    USE_AMALGAMATION=0 \
    testfixture
RUN chown -R sqlite:sqlite /home/sqlite/build
RUN rm -rf /home/sqlite/test-vfs

# Remove unix file system specific tests from wal2 that expect the db-shm to be persisted to disk
# and also care about file permissions for db-wal, db-shm and journal files. Since the test VFS
# isn't meant for saving files to disk, those tests are removed.
COPY patch/test/wal2.test.patch wal2.test.patch
RUN patch sqlite-src-3370200/test/wal2.test wal2.test.patch && rm wal2.test.patch
RUN rm sqlite-src-3370200/test/journal3.test

# Remove e_walauto.test as it requires an actually memory mapped wal index
RUN rm sqlite-src-3370200/test/e_walauto.test
RUN rm sqlite-src-3370200/test/mmapwarm.test

# Remove external_reader.test as the unix-specific SQLITE_FCNTL_EXTERNAL_READER is not implemented.
RUN rm sqlite-src-3370200/test/external_reader.test

# Remove oserror.test as it tests specifics of the default unix/windows VFS modules.
RUN rm sqlite-src-3370200/test/oserror.test

# Loading extensions is not supported
RUN rm sqlite-src-3370200/test/loadext.test
RUN rm sqlite-src-3370200/test/loadext2.test

# Remove long running tests that while being green, don't contribute tests relevant for a VFS
# implementation so it is not forth waiting for them.
RUN rm sqlite-src-3370200/test/backup_ioerr.test

# Remove tests from misc7 that only test file-system specific behaviour which don't contribute
# to the test coverage of a custom VFS such as the OS running out of file descriptors and trying
# to write a file on disk that is read-only.
COPY patch/test/misc7.test.patch misc7.test.patch
RUN patch sqlite-src-3370200/test/misc7.test misc7.test.patch && rm misc7.test.patch

# Remove tests that expect `xSetSystemCall`, which is not implemented.
RUN rm sqlite-src-3370200/test/mmap2.test
RUN rm sqlite-src-3370200/test/syscall.test
RUN rm sqlite-src-3370200/test/sysfault.test

RUN mkdir /home/sqlite/lib/
ENV LD_LIBRARY_PATH /home/sqlite/lib/

WORKDIR /github/workspace
COPY entrypoint.sh /
ENTRYPOINT ["/entrypoint.sh"]
