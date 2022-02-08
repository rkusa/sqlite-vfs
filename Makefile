PLATFORM=linux/arm64

test: test_build_image
	docker run --rm --platform linux/arm64 -t sqlite-vfs-test

test_build_image:
	docker build --platform $(PLATFORM) -f test-vfs/Dockerfile -t sqlite-vfs-test . --progress=plain
