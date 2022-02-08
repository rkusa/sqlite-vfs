# cannot use a musl based image, as the following expects dynamically linked libs
FROM rust:1-slim

# add build dependencies
RUN apt-get update && apt-get install -y \
    curl build-essential tcl-dev valgrind unzip \
    && rm -rf /var/lib/apt/lists/*

# create non-root user to execute the tests (some tests fail if executed as root)
RUN addgroup -gid 1000 --system sqlite && \
    adduser -uid 1000 --disabled-password --system --gid 1000 --home /home/sqlite --no-create-home sqlite
USER 1000
WORKDIR /home/sqlite

# build a stub of the test vfs, which allows to build and cache sqlite before building the actual
# test vfs
ADD --chown=sqlite test-vfs/docker/test-vfs /home/sqlite/sqlite-vfs/test-vfs
RUN cd sqlite-vfs/test-vfs && cargo build && mv target ../

# download and extract sqlite source
RUN curl -LO https://www.sqlite.org/2022/sqlite-src-3370200.zip && unzip sqlite-src-3370200.zip

# build sqlite test programs
RUN mkdir build && cd build && ../sqlite-src-3370200/configure
RUN cd build && make \
    OPTS="-DSQLITE_EXTRA_INIT=sqlite3_register_test_vfs" \
    LIBS="-L/home/sqlite/sqlite-vfs/target/debug/ -ltest_vfs" \
    USE_AMALGAMATION=0 \
    testfixture
RUN rm -rf /home/sqlite/sqlite-vfs/target/debug/

# build the actual test vfs
ADD --chown=sqlite Cargo.toml /home/sqlite/sqlite-vfs/Cargo.toml
ADD --chown=sqlite Cargo.lock /home/sqlite/sqlite-vfs/Cargo.lock
ADD --chown=sqlite src /home/sqlite/sqlite-vfs/src
ADD --chown=sqlite test-vfs/src /home/sqlite/sqlite-vfs/test-vfs/src
ADD --chown=sqlite test-vfs/Cargo.toml /home/sqlite/sqlite-vfs/test-vfs/Cargo.toml
RUN --mount=type=cache,uid=1000,gid=1000,target=/usr/local/cargo/git \
    --mount=type=cache,uid=1000,gid=1000,target=/usr/local/cargo/registry \
    --mount=type=cache,sharing=private,uid=1000,gid=1000,target=/home/sqlite/sqlite-vfs/target \
    cd sqlite-vfs && cargo build -p test-vfs --no-default-features

RUN --mount=type=cache,sharing=private,uid=1000,gid=1000,target=/home/sqlite/sqlite-vfs/target \
    mkdir lib && cp sqlite-vfs/target/debug/libtest_vfs.so lib/
RUN ls /home/sqlite/lib/
ENV LD_LIBRARY_PATH /home/sqlite/lib/

WORKDIR /home/sqlite/build

# s/extraquick.test/all.test/
# CMD ["valgrind", "--leak-check=full", "./testfixture", "../sqlite-src-3370200/test/extraquick.test", "--verbose=file", "--output=test-out.txt"]
CMD ["./testfixture", "../sqlite-src-3370200/test/extraquick.test", "--verbose=file", "--output=test-out.txt"]
