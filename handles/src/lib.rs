/*
 * Description: ???
 *
 * Copyright (C) 2023 Danny McClanahan <dmcC2@hypnicjerk.ai>
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (see LICENSE).
 */

//! ???

/* These clippy lint descriptions are purely non-functional and do not affect the functionality
 * or correctness of the code. */
// #![warn(missing_docs)]

/* Note: run clippy with: rustup run nightly cargo-clippy! */
#![deny(unsafe_code)]
/* Ensure any doctest warnings fails the doctest! */
#![doc(test(attr(deny(warnings))))]
/* Enable all clippy lints except for many of the pedantic ones. It's a shame this needs to be
 * copied and pasted across crates, but there doesn't appear to be a way to include inner
 * attributes from a common source. */
#![deny(
  clippy::all,
  clippy::default_trait_access,
  clippy::expl_impl_clone_on_copy,
  clippy::if_not_else,
  clippy::needless_continue,
  clippy::single_match_else,
  clippy::unseparated_literal_suffix,
  clippy::used_underscore_binding
)]
/* It is often more clear to show that nothing is being moved. */
#![allow(clippy::match_ref_pats)]
/* Subjective style. */
#![allow(
  clippy::derived_hash_with_manual_eq,
  clippy::len_without_is_empty,
  clippy::redundant_field_names,
  clippy::too_many_arguments,
  clippy::single_component_path_imports,
  clippy::double_must_use
)]
/* Default isn't as big a deal as people seem to think it is. */
#![allow(clippy::new_without_default, clippy::new_ret_no_self)]
/* Arc<Mutex> can be more clear than needing to grok Orderings. */
#![allow(clippy::mutex_atomic)]

/* TODO: stop exposing any remaining parts of the unstable pants rust API directly! */

pub use fs;
pub use hashing;
pub use store;
pub use task_executor::Executor;

use async_trait::async_trait;
use bytes::Bytes;
use displaydoc::Display;
/* use parking_lot::{Mutex, RwLock}; */
use thiserror::Error;
use tokio::task::JoinError;

use std::{
  io,
  marker::Unpin,
  path::{Path, PathBuf},
  pin::pin,
  sync::Arc,
};

#[derive(Debug, Display, Error)]
pub enum HandleError {
  /// error string: {0}
  StringError(String),
  /// inner store error: {0:?}
  Store(store::StoreError),
  /// io error: {0}
  Io(#[from] io::Error),
  /// join error: {0}
  Join(#[from] JoinError),
}

impl From<String> for HandleError {
  fn from(s: String) -> Self {
    Self::StringError(s)
  }
}

impl From<store::StoreError> for HandleError {
  fn from(e: store::StoreError) -> Self {
    Self::Store(e)
  }
}

pub trait HasExecutor {
  fn executor(&self) -> &Executor;
  fn create_vfs(&self, root: &Path) -> Result<fs::PosixFS, HandleError> {
    Ok(fs::PosixFS::new_with_symlink_behavior(
      root,
      fs::GitignoreStyleExcludes::empty(),
      self.executor().clone(),
      fs::SymlinkBehavior::Oblivious,
    )?)
  }
}

#[async_trait]
pub trait Crawler {
  async fn expand_globs(
    &self,
    root: &Path,
    path_globs: fs::PreparedPathGlobs,
  ) -> Result<store::Snapshot, HandleError>;
}

#[async_trait]
pub trait ByteStore: HasExecutor {
  async fn store_small_bytes(
    &self,
    bytes: Bytes,
    initial_lease: bool,
  ) -> Result<hashing::Digest, HandleError>;

  async fn store_small_bytes_batch(
    &self,
    items: Vec<(hashing::Fingerprint, Bytes)>,
    initial_lease: bool,
  ) -> Result<(), HandleError>;

  async fn store_streaming_file(
    &self,
    initial_lease: bool,
    data_is_immutable: bool,
    src: PathBuf,
  ) -> Result<hashing::Digest, HandleError>;

  async fn store_byte_stream<R: io::Read + Send + Unpin + 'static>(
    &self,
    initial_lease: bool,
    src: R,
  ) -> Result<hashing::Digest, HandleError> {
    /* FIXME: implement this without an intermediate temporary file! */
    use tempfile::NamedTempFile;

    let tmp_out = NamedTempFile::new()?;
    let tmp_out_path = tmp_out.path().to_path_buf();

    self
      .executor()
      .native_spawn_blocking(move || {
        let src = pin!(src);
        let tmp_out = pin!(tmp_out);
        io::copy(&mut *src.get_mut(), &mut *tmp_out.get_mut())?;
        Ok::<(), io::Error>(())
      })
      .await??;

    Ok(
      self
        .store_streaming_file(initial_lease, true, tmp_out_path)
        .await?,
    )
  }

  async fn remove_entry(&self, digest: hashing::Digest) -> Result<bool, HandleError>;

  async fn load_file_bytes_with<
    T: Send + 'static,
    F: Fn(&[u8]) -> T + Clone + Send + Sync + 'static,
  >(
    &self,
    digest: hashing::Digest,
    f: F,
  ) -> Result<T, HandleError>;
}

#[async_trait]
pub trait DirectoryStore {
  async fn snapshot_of_one_file(
    &self,
    name: fs::RelativePath,
    digest: hashing::Digest,
    is_executable: bool,
  ) -> Result<store::Snapshot, HandleError>;

  async fn record_digest_trie(
    &self,
    tree: fs::DigestTrie,
    initial_lease: bool,
  ) -> Result<fs::DirectoryDigest, HandleError>;

  async fn load_digest_trie(
    &self,
    digest: fs::DirectoryDigest,
  ) -> Result<fs::DigestTrie, HandleError>;

  async fn load_directory_digest(
    &self,
    digest: hashing::Digest,
  ) -> Result<fs::DirectoryDigest, HandleError>;
}

pub struct Handles {
  executor: Executor,
  fs_store: store::Store,
}

impl Handles {
  pub fn new(local_store_path: impl AsRef<Path>) -> Result<Self, String> {
    let exe = Executor::new();
    let local_store = store::Store::local_only(exe.clone(), local_store_path)?;
    Ok(Self {
      executor: exe,
      fs_store: local_store,
    })
  }
}

impl HasExecutor for Handles {
  fn executor(&self) -> &Executor {
    &self.executor
  }
}

#[async_trait]
impl Crawler for Handles {
  async fn expand_globs(
    &self,
    root: &Path,
    path_globs: fs::PreparedPathGlobs,
  ) -> Result<store::Snapshot, HandleError> {
    use fs::GlobMatching;

    let vfs = Arc::new(self.create_vfs(root)?);
    let path_stats = vfs
      .expand_globs(path_globs, fs::SymlinkBehavior::Oblivious, None)
      .await
      .map_err(|err| format!("Error expanding globs: {err}"))?;
    Ok(
      store::Snapshot::from_path_stats(
        store::OneOffStoreFileByDigest::new(self.fs_store.clone(), vfs, true),
        path_stats,
      )
      .await?,
    )
  }
}

#[async_trait]
impl ByteStore for Handles {
  async fn store_small_bytes(
    &self,
    bytes: Bytes,
    initial_lease: bool,
  ) -> Result<hashing::Digest, HandleError> {
    Ok(self.fs_store.store_file_bytes(bytes, initial_lease).await?)
  }

  async fn store_small_bytes_batch(
    &self,
    items: Vec<(hashing::Fingerprint, Bytes)>,
    initial_lease: bool,
  ) -> Result<(), HandleError> {
    Ok(
      self
        .fs_store
        .store_file_bytes_batch(items, initial_lease)
        .await?,
    )
  }

  async fn store_streaming_file(
    &self,
    initial_lease: bool,
    data_is_immutable: bool,
    src: PathBuf,
  ) -> Result<hashing::Digest, HandleError> {
    Ok(
      self
        .fs_store
        .store_file(initial_lease, data_is_immutable, src)
        .await?,
    )
  }

  async fn remove_entry(&self, digest: hashing::Digest) -> Result<bool, HandleError> {
    Ok(self.fs_store.remove_file(digest).await?)
  }

  async fn load_file_bytes_with<
    T: Send + 'static,
    F: Fn(&[u8]) -> T + Clone + Send + Sync + 'static,
  >(
    &self,
    digest: hashing::Digest,
    f: F,
  ) -> Result<T, HandleError> {
    Ok(
      self
        .fs_store
        .load_file_bytes_with::<T, F>(digest, f)
        .await?,
    )
  }
}

#[async_trait]
impl DirectoryStore for Handles {
  async fn snapshot_of_one_file(
    &self,
    name: fs::RelativePath,
    digest: hashing::Digest,
    is_executable: bool,
  ) -> Result<store::Snapshot, HandleError> {
    Ok(
      self
        .fs_store
        .snapshot_of_one_file(name, digest, is_executable)
        .await?,
    )
  }

  async fn record_digest_trie(
    &self,
    tree: fs::DigestTrie,
    initial_lease: bool,
  ) -> Result<fs::DirectoryDigest, HandleError> {
    Ok(
      self
        .fs_store
        .record_digest_trie(tree, initial_lease)
        .await?,
    )
  }

  async fn load_digest_trie(
    &self,
    digest: fs::DirectoryDigest,
  ) -> Result<fs::DigestTrie, HandleError> {
    Ok(self.fs_store.load_digest_trie(digest).await?)
  }

  async fn load_directory_digest(
    &self,
    digest: hashing::Digest,
  ) -> Result<fs::DirectoryDigest, HandleError> {
    Ok(self.fs_store.load_directory_digest(digest).await?)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  use tempfile::tempdir;

  #[tokio::test]
  async fn it_works() {
    let local_store_path = tempdir().unwrap();
    let _handles = Handles::new(local_store_path.path()).unwrap();
  }
}
