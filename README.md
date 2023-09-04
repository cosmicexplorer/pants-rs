symbolic-fs
===========

*Performant parallelized operations on virtual directory trees.*

This is a collection of crates wrapping [pants](https://github.com/pantsbuild/pants) Rust code. This is an unstable API (pants does not support use as a library), so we won't expose any pants code as a public API.

# TODO
- [ ] Migrate [upc](https://github.com/cosmicexplorer/upc)'s Rust FFI to call into crates from this workspace.
- [ ] Integrate [`tracing`](https://github.com/tokio-rs/tracing) with `workunit_store` (?).

# License
[Apache v2](./LICENSE).
