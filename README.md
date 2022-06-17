# `sqlite-vfs`

Build SQLite virtual file systems (VFS) by implementing a simple Rust trait.

[Documentation](https://docs.rs/sqlite-vfs) | [Example](https://github.com/rkusa/sqlite-vfs/blob/main/examples/fs.rs)

This library is build for my own use-case. It doesn't expose everything a SQLite VFS provides (e.g. memory mapped files). Feel free to propose additions if the current state doesn't work for your use-case.

## Status

This library is still in _prototype_ state and not ready to be used (except for maybe prototypes). While progress will be slow, it is actively worked on.

- ✅ Good enough for single-threaded experiments like [`do-sqlite`](https://github.com/rkusa/do-sqlite).
- ❌ It is not passing the SQLite's TCL test harness yet (WIP: [#1](https://github.com/rkusa/sqlite-vfs/pull/1)).
- ⚠️ It uses `unsafe` Rust, which hasn't been peer-reviewed yet.
- ⚠️ It is not used in any production-capacity yet.

## Limitations

- Memory mapping not supported (`xFetch`/`xUnfetch`)
- Loading extensions not supported (`xDl*`)
- Tests run only on UNIX right now (due to `std::os::unix` usage in tests)
