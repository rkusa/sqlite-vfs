# `sqlite-vfs`

Build SQLite virtual file systems (VFS) by implementing a simple Rust trait.

[Documentation](https://docs.rs/sqlite-vfs) | [Example](https://github.com/rkusa/sqlite-vfs/blob/main/examples/fs.rs)

This library is build for my own use-case. It doesn't expose everything a SQLite VFS provides (e.g. memory mapped files). Feel free to propose additions if the current state doesn't work for your use-case.

**Disclaimer:** This library uses _unsafe_ Rust to call SQLite C functions. I am neither an SQLite nor a _unsafe_ Rust expert. I am only using this library for experiments (and not in any production capacity) right now.
