PLATFORM=linux/arm64

test: test-vfs/.dockerbuild
	mkdir -p $(shell pwd)/target/x86_64-unknown-linux-gnu
	docker run --rm --platform $(PLATFORM) \
		--mount type=bind,src=$(shell pwd),dst=/github/workspace,readonly \
		--mount type=bind,src=${HOME}/.cargo/git,dst=/usr/local/cargo/git,readonly \
		--mount type=bind,src=${HOME}/.cargo/registry,dst=/usr/local/cargo/registry,readonly \
		--mount type=bind,src=$(shell pwd)/target/x86_64-unknown-linux-gnu,dst=/github/workspace/target \
		-t sqlite-vfs-test

test-vfs/.dockerbuild: test-vfs/Dockerfile test-vfs/entrypoint.sh test-vfs/docker/test-vfs/Cargo.toml test-vfs/docker/test-vfs/src/*.rs
	docker build --platform $(PLATFORM) \
		-f test-vfs/Dockerfile \
		--progress=plain \
		-t sqlite-vfs-test test-vfs
	touch test-vfs/.dockerbuild