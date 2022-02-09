PLATFORM=linux/arm64

test: test_build_image
	docker run --rm --platform $(PLATFORM) \
		--mount type=bind,src=$(shell pwd),dst=/github/workspace,readonly \
		--mount type=bind,src=${HOME}/.cargo/git,dst=/usr/local/cargo/git,readonly \
		--mount type=bind,src=${HOME}/.cargo/registry,dst=/usr/local/cargo/registry,readonly \
		--tmpfs /github/workspace/target:exec \
		-t sqlite-vfs-test

test_build_image:
	docker build --platform $(PLATFORM) \
		-f test-vfs/Dockerfile \
		--progress=plain \
		-t sqlite-vfs-test test-vfs
