# cannot use a musl based image, as the following expects dynamically linked libs
FROM rust:1-slim

# add build dependencies
RUN apt-get update && apt-get install -y \
    curl build-essential tcl-dev valgrind unzip \
    && rm -rf /var/lib/apt/lists/*

# create non-root user to execute the tests (some tests fail if executed as root)
ARG UID=1000
RUN addgroup -gid 1000 --system sqlite && \
    adduser -uid $UID --disabled-password --system --gid 1000 --home /home/sqlite sqlite
USER $UID
WORKDIR /home/sqlite

# build a stub of the test vfs, which allows to build and cache sqlite before building the actual
# test vfs
ADD --chown=sqlite docker/test-vfs /home/sqlite/test-vfs
RUN cd test-vfs && cargo build

# download and extract sqlite source
RUN curl -LO https://www.sqlite.org/2022/sqlite-src-3370200.zip && unzip sqlite-src-3370200.zip

# build sqlite test programs
RUN mkdir build && cd build && ../sqlite-src-3370200/configure
RUN cd build && make \
    OPTS="-DSQLITE_EXTRA_INIT=sqlite3_register_test_vfs" \
    LIBS="-L/home/sqlite/test-vfs/target/debug/ -ltest_vfs" \
    USE_AMALGAMATION=0 \
    testfixture
RUN rm -rf /home/sqlite/test-vfs

RUN mkdir /home/sqlite/lib/
ENV LD_LIBRARY_PATH /home/sqlite/lib/

WORKDIR /github/workspace
COPY entrypoint.sh /
ENTRYPOINT ["/entrypoint.sh"]