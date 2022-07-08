# `sqlite-vfs`

Build SQLite virtual file systems (VFS) by implementing a simple Rust trait.

[Documentation](https://docs.rs/sqlite-vfs) | [Example](https://github.com/rkusa/wasm-sqlite/blob/main/wasm/src/vfs.rs)

This library is build for my own use-case. It doesn't expose everything a SQLite VFS provides (e.g. memory mapped files). Feel free to propose additions if the current state doesn't work for your use-case.

## Status

This library is still in _prototype_ state and not ready to be used (except for maybe prototypes). While progress will be slow, it is actively worked on.

- ✅ It passes most of SQLite's TCL test harness.
  - ⚠️ CI only runs `full.test` and not `all.test`.
  - ⚠️ [Some tests](./test-vfs/patch.sh) are skipped.
- ✅ Successfully runs experiments like [`do-sqlite`](https://github.com/rkusa/do-sqlite).
- ⚠️ It uses `unsafe` Rust, which hasn't been peer-reviewed yet.
- ⚠️ It is not used in any production-capacity yet.

## Limitations

- WAL is not supported (but in progress)
- Memory mapping not supported (`xFetch`/`xUnfetch`)
- Loading extensions not supported (`xDl*`)
- Tests run only on UNIX right now (due to `std::os::unix` usage in tests)
- Directory sync is not supported
- Sector size is always 1024
- Custom device characteristic are not supported (`xDeviceCharacteristics`)
