PLATFORM=linux/arm64

test: test_build_image
	docker run --rm --platform $(PLATFORM) \
		--user $(shell id -u):$(shell id -g) \
		--mount type=bind,src=$(shell pwd),dst=/github/workspace \
		--mount type=bind,src=${HOME}/.cargo/git,dst=/usr/local/cargo/git \
		--mount type=bind,src=${HOME}/.cargo/registry,dst=/usr/local/cargo/registry \
		--tmpfs /github/workspace/target:uid=$(shell id -u),gid=$(shell id -g),exec \
		-t sqlite-vfs-test

test_build_image:
	docker build --platform $(PLATFORM) \
		-f test-vfs/Dockerfile \
		--progress=plain \
		--build-arg UID=$(shell id -u) \
		-t sqlite-vfs-test test-vfs
