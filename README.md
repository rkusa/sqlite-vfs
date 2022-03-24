# `sqlite-vfs`

Build SQLite virtual file systems (VFS) by implementing a simple Rust trait.

Originally copied from https://github.com/rkusa/sqlite-vfs , but modified to be wasm compatible.

- Most things from sqlite VFS are exposed, so e.g. you can use a wasm specific way to get randomness

- No types from std that are not available from wasm, such as std::path::Path, are used

- Types are kept low level (e.g. CStr). Error codes are just from sqlite-sys.