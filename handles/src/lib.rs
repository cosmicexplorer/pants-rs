/*
 * Description: A set of utilities to symbolically manipulate file contents and directory trees by
 *              entering their contents into a content-addressed store.
 *
 * Copyright (C) 2023 Danny McClanahan <dmcC2@hypnicjerk.ai>
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (see LICENSE).
 */

//! A set of utilities to symbolically manipulate file contents and directory trees by entering
//! their contents into a content-addressed store.
//!
//! **TODO: support for remote sync!**

/* These clippy lint descriptions are purely non-functional and do not affect the functionality
 * or correctness of the code. */
#![warn(missing_docs)]
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

use async_stream::try_stream;
pub use bytes::Bytes;
use displaydoc::Display;
use futures_util::pin_mut;
use tempfile;
use thiserror::Error;
use tokio::{task, task::JoinError};
pub use tokio_stream::Stream;
use tokio_stream::StreamExt;
pub use zip;
use zip::{result::ZipError, DateTime};

use std::{
  collections::HashMap,
  future,
  io::{self, Write},
  marker::PhantomData,
  marker::Unpin,
  mem, ops,
  path::{Path, PathBuf},
  pin::{self, Pin},
  sync::Arc,
};

/// Top-level errors produced by this crate.
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
  /// zip error: {0}
  Zip(#[from] ZipError),
  /// path transform error: {0}
  PathTransform(#[from] PathTransformError),
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

/// Errors when transforming file paths between contexts.
#[derive(Debug, Display, Error)]
pub enum PathTransformError {
  /// Zip entry name was invalid: {0}
  MalformedZipName(String),
}

trait HasExecutor {
  fn executor(&self) -> &Executor;
}

trait HasStore {
  fn store(&self) -> &store::Store;
}

/// Efficiently traverse the local disk and enter file contents in the persistent byte store.
#[derive(Debug, Clone)]
pub struct Crawler {
  handles: Handles,
}

impl Crawler {
  pub(crate) fn new(handles: Handles) -> Self {
    Self { handles }
  }

  /// Crawl the filesystem and enter every matching entry into the local store.
  ///
  ///```
  /// # fn main() -> Result<(), executor_resource_handles::HandleError> { tokio_test::block_on(async {
  /// use executor_resource_handles::Handles;
  /// use tempfile::tempdir;
  /// use std::{path::PathBuf, str::FromStr};
  ///
  /// let store_td = tempdir()?;
  /// let handles = Handles::new(store_td.path())?;
  /// let crawler = handles.crawler();
  ///
  /// let td = tempdir()?;
  /// std::fs::write(td.path().join("a.txt"), "wow\n")?;
  /// std::fs::write(td.path().join("b.txt"), "hey\n")?;
  ///
  /// let globs = fs::PreparedPathGlobs::create(
  ///   vec!["*.txt".to_string()],
  ///   fs::StrictGlobMatching::Ignore,
  ///   fs::GlobExpansionConjunction::AnyMatch,
  /// )?;
  /// let snapshot = crawler.expand_globs(td.path(), globs).await?;
  /// assert_eq!(snapshot.files(), vec![PathBuf::from("a.txt"), PathBuf::from("b.txt")]);
  /// let dir_digest: fs::DirectoryDigest = snapshot.into();
  /// let digest = dir_digest.as_digest();
  /// let fp: hashing::Fingerprint =
  ///   hashing::Fingerprint::from_str("ba3e9666bbee9d4890698773b29532302024f40d0ce4764cfe4c4e91f5517be6")?;
  /// assert_eq!(digest, hashing::Digest { hash: fp, size_bytes: 158 });
  /// # Ok(())
  /// # })}
  ///```
  pub async fn expand_globs(
    &self,
    root: impl AsRef<Path>,
    path_globs: fs::PreparedPathGlobs,
  ) -> Result<store::Snapshot, HandleError> {
    use fs::GlobMatching;

    let vfs = Arc::new(self.create_vfs(root.as_ref())?);
    let path_stats = vfs
      .expand_globs(path_globs, fs::SymlinkBehavior::Aware, None)
      .await
      .map_err(|err| format!("Error expanding globs: {err}"))?;
    Ok(
      store::Snapshot::from_path_stats(
        store::OneOffStoreFileByDigest::new(self.handles.store().clone(), vfs, true),
        path_stats,
      )
      .await?,
    )
  }

  fn create_vfs(&self, root: &Path) -> Result<fs::PosixFS, HandleError> {
    Ok(fs::PosixFS::new_with_symlink_behavior(
      root,
      /* TODO: add this as an option when creating a Handles! */
      fs::GitignoreStyleExcludes::empty(),
      self.handles.executor().clone(),
      /* TODO: add this as an option when creating a Handles! */
      fs::SymlinkBehavior::Oblivious,
    )?)
  }
}

/// Enter and retrieve byte streams to and from the local store.
#[derive(Debug, Clone)]
pub struct ByteStore {
  handles: Handles,
}

impl HasStore for ByteStore {
  fn store(&self) -> &store::Store {
    self.handles.store()
  }
}

impl ByteStore {
  pub(crate) fn new(handles: Handles) -> Self {
    Self { handles }
  }

  /// Store a small byte chunk.
  ///
  /// *NB: Use [`Self::store_streaming_file`] to stream in larger files.*
  ///
  ///```
  /// # fn main() -> Result<(), executor_resource_handles::HandleError> { tokio_test::block_on(async {
  /// use executor_resource_handles::Handles;
  /// use bytes::Bytes;
  /// use tempfile::tempdir;
  /// use std::str::FromStr;
  ///
  /// let store_td = tempdir()?;
  /// let handles = Handles::new(store_td.path())?;
  /// let byte_store = handles.byte_store();
  ///
  /// let msg = Bytes::copy_from_slice(b"wow\n");
  /// let digest = byte_store.store_small_bytes(msg, true).await?;
  ///
  /// let fp: hashing::Fingerprint =
  ///   hashing::Fingerprint::from_str("f40cd21f276e47d533371afce1778447e858eb5c9c0c0ed61c65f5c5d57caf63")?;
  /// assert_eq!(digest, hashing::Digest { hash: fp, size_bytes: 4 });
  ///
  /// # Ok(())
  /// # })}
  ///```
  pub async fn store_small_bytes(
    &self,
    bytes: Bytes,
    initial_lease: bool,
  ) -> Result<hashing::Digest, HandleError> {
    Ok(self.store().store_file_bytes(bytes, initial_lease).await?)
  }

  /// Store multiple small byte chunks.
  ///
  ///```
  /// # fn main() -> Result<(), executor_resource_handles::HandleError> { tokio_test::block_on(async {
  /// use executor_resource_handles::Handles;
  /// use bytes::Bytes;
  /// use tempfile::tempdir;
  /// use std::str::FromStr;
  ///
  /// let store_td = tempdir()?;
  /// let handles = Handles::new(store_td.path())?;
  /// let byte_store = handles.byte_store();
  ///
  /// let msg = Bytes::copy_from_slice(b"wow\n");
  /// let fp: hashing::Fingerprint =
  ///   hashing::Fingerprint::from_str("f40cd21f276e47d533371afce1778447e858eb5c9c0c0ed61c65f5c5d57caf63")?;
  /// let msg2 = Bytes::copy_from_slice(b"hey\n");
  /// let fp2: hashing::Fingerprint =
  ///   hashing::Fingerprint::from_str("4e955fea0268518cbaa500409dfbec88f0ecebad28d84ecbe250baed97dba889")?;
  ///
  /// byte_store.store_small_bytes_batch(vec![(fp, msg.clone()), (fp2, msg2.clone())], true).await?;
  ///
  /// let digest = hashing::Digest { hash: fp, size_bytes: 4 };
  /// let digest2 = hashing::Digest { hash: fp2, size_bytes: 4 };
  ///
  /// assert_eq!(msg, byte_store.load_file_bytes_with(digest, Bytes::copy_from_slice).await?);
  /// assert_eq!(msg2, byte_store.load_file_bytes_with(digest2, Bytes::copy_from_slice).await?);
  /// # Ok(())
  /// # })}
  ///```
  pub async fn store_small_bytes_batch(
    &self,
    items: Vec<(hashing::Fingerprint, Bytes)>,
    initial_lease: bool,
  ) -> Result<(), HandleError> {
    Ok(
      self
        .store()
        .store_file_bytes_batch(items, initial_lease)
        .await?,
    )
  }

  /// Stream in the contents of `src` to the local store.
  ///
  ///```
  /// # fn main() -> Result<(), executor_resource_handles::HandleError> { tokio_test::block_on(async {
  /// use executor_resource_handles::Handles;
  /// use tempfile::tempdir;
  /// use std::str::FromStr;
  ///
  /// let store_td = tempdir()?;
  /// let handles = Handles::new(store_td.path())?;
  /// let byte_store = handles.byte_store();
  ///
  /// let td = tempdir()?;
  /// let out_path = td.path().join("c.txt");
  /// std::fs::write(&out_path, b"hello!!!\n")?;
  ///
  /// let resulting_digest = byte_store.store_streaming_file(true, true, out_path).await?;
  ///
  /// let fp = hashing::Fingerprint::from_str("d1d23437b40e63d96c9bdd7458e1a61a72b70910d4f053744303f5165d64cbfd")?;
  /// let digest = hashing::Digest { hash: fp, size_bytes: 9 };
  /// assert_eq!(resulting_digest, digest);
  /// # Ok(())
  /// # })}
  ///```
  pub async fn store_streaming_file(
    &self,
    initial_lease: bool,
    data_is_immutable: bool,
    src: PathBuf,
  ) -> Result<hashing::Digest, HandleError> {
    Ok(
      self
        .store()
        .store_file(initial_lease, data_is_immutable, src)
        .await?,
    )
  }

  /// Remove the entry for `digest` from the local store.
  ///
  ///```
  /// # use executor_resource_handles::HandleError;
  /// # fn main() -> Result<(), executor_resource_handles::HandleError> { tokio_test::block_on(async {
  /// use executor_resource_handles::Handles;
  /// use bytes::Bytes;
  /// use tempfile::tempdir;
  /// use std::str::FromStr;
  ///
  /// let store_td = tempdir()?;
  /// let handles = Handles::new(store_td.path())?;
  /// let byte_store = handles.byte_store();
  ///
  /// let msg = Bytes::copy_from_slice(b"wow\n");
  /// let digest = byte_store.store_small_bytes(msg.clone(), true).await?;
  ///
  /// let fp: hashing::Fingerprint =
  ///   hashing::Fingerprint::from_str("f40cd21f276e47d533371afce1778447e858eb5c9c0c0ed61c65f5c5d57caf63")?;
  /// assert_eq!(digest, hashing::Digest { hash: fp, size_bytes: 4 });
  ///
  /// assert_eq!(msg, byte_store.load_file_bytes_with(digest, Bytes::copy_from_slice).await?);
  ///
  /// assert!(byte_store.remove_entry(digest).await?);
  ///
  /// match byte_store.load_file_bytes_with(digest, Bytes::copy_from_slice).await {
  ///   Err(HandleError::Store(store::StoreError::MissingDigest(_, _))) => (),
  ///   _ => unreachable!(),
  /// }
  /// # Ok(())
  /// # })}
  ///```
  pub async fn remove_entry(&self, digest: hashing::Digest) -> Result<bool, HandleError> {
    Ok(self.store().remove_file(digest).await?)
  }

  /// Extract the byte string corresponding to `digest` from the local store.
  pub async fn load_file_bytes_with<
    T: Send + 'static,
    F: Fn(&[u8]) -> T + Clone + Send + Sync + 'static,
  >(
    &self,
    digest: hashing::Digest,
    f: F,
  ) -> Result<T, HandleError> {
    Ok(self.store().load_file_bytes_with::<T, F>(digest, f).await?)
  }

  /// Enter an arbitrary readable stream in the local store by writing it to a temporary file.
  ///
  ///```
  /// # use executor_resource_handles::HandleError;
  /// # fn main() -> Result<(), HandleError> { tokio_test::block_on(async {
  /// use executor_resource_handles::Handles;
  /// use tempfile::tempdir;
  /// use std::str::FromStr;
  ///
  /// let store_td = tempdir()?;
  /// let handles = Handles::new(store_td.path())?;
  /// let byte_store = handles.byte_store();
  ///
  /// let td = tempdir()?;
  /// let file_path = td.path().join("asdf.txt");
  ///
  /// let digest = {
  ///   std::fs::write(&file_path, b"wowowow\n")?;
  ///   let f = std::fs::OpenOptions::new().read(true).open(&file_path)?;
  ///   byte_store.store_byte_stream(true, f).await?
  /// };
  /// let fp =
  ///   hashing::Fingerprint::from_str("9da8e466dd1600e44f79f691d111d7d95dd02c70f8d92328fca0653daba85ae8")?;
  /// assert_eq!(digest, hashing::Digest { hash: fp, size_bytes: 8 });
  /// # Ok(())
  /// # })}
  ///```
  pub async fn store_byte_stream<R: io::Read + Send + Unpin>(
    &self,
    initial_lease: bool,
    src: R,
  ) -> Result<hashing::Digest, HandleError> {
    /* FIXME: implement this without an intermediate temporary file! */
    let tmp_out = Reader::new(src).expand_reader().await?;
    let tmp_out_path = tmp_out.to_path_buf();
    Ok(
      self
        .store_streaming_file(initial_lease, true, tmp_out_path)
        .await?,
    )
  }

  /// Enter an arbitrary readable stream in the local store by writing it to a temporary file.
  ///
  ///```
  /// # use executor_resource_handles::HandleError;
  /// # fn main() -> Result<(), HandleError> { tokio_test::block_on(async {
  /// use executor_resource_handles::Handles;
  /// use tempfile::tempdir;
  /// use std::str::FromStr;
  ///
  /// let store_td = tempdir()?;
  /// let handles = Handles::new(store_td.path())?;
  /// let byte_store = handles.byte_store();
  ///
  /// let td = tempdir()?;
  /// let file_path = td.path().join("asdf.txt");
  ///
  /// let digest = {
  ///   std::fs::write(&file_path, b"wowowow\n")?;
  ///   let f = tokio::fs::OpenOptions::new().read(true).open(&file_path).await?;
  ///   byte_store.store_async_byte_stream(true, f).await?
  /// };
  /// let fp =
  ///   hashing::Fingerprint::from_str("9da8e466dd1600e44f79f691d111d7d95dd02c70f8d92328fca0653daba85ae8")?;
  /// assert_eq!(digest, hashing::Digest { hash: fp, size_bytes: 8 });
  /// # Ok(())
  /// # })}
  ///```
  pub async fn store_async_byte_stream<R: tokio::io::AsyncRead>(
    &self,
    initial_lease: bool,
    src: R,
  ) -> Result<hashing::Digest, HandleError> {
    let tmp_out = Reader::new(src).expand_async_reader().await?;
    let tmp_out_path = tmp_out.to_path_buf();
    Ok(
      self
        .store_streaming_file(initial_lease, true, tmp_out_path)
        .await?,
    )
  }
}

struct BoxLeakChannel<I> {
  args: usize,
  _ph: PhantomData<I>,
}

impl<I> BoxLeakChannel<I> {
  pub fn new(args: I) -> Self {
    Self {
      args: Box::into_raw(Box::new(args)) as usize,
      _ph: PhantomData,
    }
  }
}

impl<I> ops::Drop for BoxLeakChannel<I> {
  fn drop(&mut self) {
    if self.args != 0 {
      let _: Box<I> = unsafe { Box::from_raw(self.args as *mut I) };
    }
  }
}

struct LeakHandle {
  inner: usize,
}

impl LeakHandle {
  pub fn new(inner: usize) -> Self {
    Self { inner }
  }
}

pub trait LeakRef<T> {
  fn leak_ref<'a>(self) -> &'a T;
  fn leak_mut<'a>(self) -> &'a mut T;
}

impl<T> LeakRef<T> for LeakHandle {
  fn leak_ref<'a>(self) -> &'a T {
    self.leak_mut::<'a>()
  }

  fn leak_mut<'a>(self) -> &'a mut T {
    let Self { inner } = self;
    let inner: Box<T> = unsafe { Box::from_raw(inner as *mut T) };
    Box::leak(inner)
  }
}

impl<I> BoxLeakChannel<I> {
  pub async fn run_leak<O, F>(self, f: F) -> O::Output
  where
    O: future::Future,
    F: FnOnce(LeakHandle) -> O,
  {
    let leak = LeakHandle::new(self.args);
    f(leak).await
  }

  pub async fn run_boxed<O, F>(mut self, f: F) -> O::Output
  where
    O: future::Future,
    F: FnOnce(I) -> O,
  {
    let args: Box<I> = unsafe { Box::from_raw(self.args as *mut I) };
    self.args = 0;
    f(*args).await
  }

  pub async fn run_mut<'a, O, F>(self, f: F) -> O::Output
  where
    O: future::Future,
    F: FnOnce(&'a mut I) -> O,
    I: 'a,
  {
    let args: Box<I> = unsafe { Box::from_raw(self.args as *mut I) };
    let args: &'a mut I = Box::leak(args);
    f(args).await
  }

  pub async fn run_pinned<'a, O, F>(self, f: F) -> O::Output
  where
    O: future::Future,
    F: FnOnce(Pin<&'a mut I>) -> O,
    I: 'a,
  {
    let args: Box<I> = unsafe { Box::from_raw(self.args as *mut I) };
    let args: &'a mut I = Box::leak(args);
    let args: Pin<&'a mut I> = unsafe { Pin::new_unchecked(args) };
    f(args).await
  }
}

struct Reader<R> {
  inner: BoxLeakChannel<R>,
}

impl<R> Reader<R> {
  pub fn new(inner: R) -> Self {
    Self {
      inner: BoxLeakChannel::new(inner),
    }
  }
}

impl<R: tokio::io::AsyncRead> Reader<R> {
  pub async fn expand_async_reader(self) -> Result<tempfile::TempPath, HandleError> {
    self
      .inner
      .run_pinned(|mut src: Pin<&mut R>| async move {
        let (tmp_out, out_path) = tempfile::NamedTempFile::new()?.into_parts();
        let mut tmp_out = tokio::fs::File::from_std(tmp_out);
        tokio::io::copy(&mut src, &mut tmp_out).await?;
        Ok::<_, HandleError>(out_path)
      })
      .await
  }
}

impl<R: io::Read> Reader<R> {
  pub async fn expand_reader(self) -> Result<tempfile::TempPath, HandleError> {
    self
      .inner
      .run_leak(|src: LeakHandle| async move {
        task::spawn_blocking(move || {
          let src: &mut R = src.leak_mut();
          let (mut tmp_out, out_path) = tempfile::NamedTempFile::new()?.into_parts();
          io::copy(src, &mut tmp_out)?;
          Ok::<_, HandleError>(out_path)
        })
        .await?
      })
      .await
  }
}

/// Compose byte contents with relative paths to form virtual directory trees.
#[derive(Debug, Clone)]
pub struct DirectoryStore {
  byte_store: ByteStore,
}

impl HasStore for DirectoryStore {
  fn store(&self) -> &store::Store {
    self.byte_store.store()
  }
}

impl DirectoryStore {
  pub(crate) fn new(byte_store: ByteStore) -> Self {
    Self { byte_store }
  }

  /// Assign the file contents from `digest` to the file at `name`.
  ///
  ///```
  /// # use executor_resource_handles::HandleError;
  /// # fn main() -> Result<(), HandleError> { tokio_test::block_on(async {
  /// use executor_resource_handles::Handles;
  /// use bytes::Bytes;
  /// use tempfile::tempdir;
  /// use std::path::PathBuf;
  ///
  /// let store_td = tempdir()?;
  /// let handles = Handles::new(store_td.path())?;
  /// let byte_store = handles.byte_store();
  /// let dir_store = handles.directory_store();
  ///
  /// let msg = Bytes::copy_from_slice(b"wow\n");
  /// let digest = byte_store.store_small_bytes(msg.clone(), true).await?;
  ///
  /// let name = fs::RelativePath::new("asdf.txt")?;
  /// let snapshot = dir_store.snapshot_of_one_file(name, digest, false).await?;
  /// assert_eq!(snapshot.files(), vec![PathBuf::from("asdf.txt")]);
  /// assert!(snapshot.directories().is_empty());
  /// let dir_digest: fs::DirectoryDigest = snapshot.into();
  /// assert_eq!(digest, dir_digest.digests()[0]);
  /// assert_eq!(2, dir_digest.digests().len());
  /// # Ok(())
  /// # })}
  ///```
  pub async fn snapshot_of_one_file(
    &self,
    name: fs::RelativePath,
    digest: hashing::Digest,
    is_executable: bool,
  ) -> Result<store::Snapshot, HandleError> {
    Ok(
      self
        .store()
        .snapshot_of_one_file(name, digest, is_executable)
        .await?,
    )
  }

  /// Recursively store all components of `tree` in the local store.
  ///
  ///```
  /// # use executor_resource_handles::HandleError;
  /// # fn main() -> Result<(), HandleError> { tokio_test::block_on(async {
  /// use executor_resource_handles::Handles;
  /// use bytes::Bytes;
  /// use tempfile::tempdir;
  /// use std::{path::{Path, PathBuf}, str::FromStr};
  ///
  /// let store_td = tempdir()?;
  /// let handles = Handles::new(store_td.path())?;
  /// let byte_store = handles.byte_store();
  /// let dir_store = handles.directory_store();
  ///
  /// let msg = Bytes::copy_from_slice(b"wow\n");
  /// let fp: hashing::Fingerprint =
  ///   hashing::Fingerprint::from_str("f40cd21f276e47d533371afce1778447e858eb5c9c0c0ed61c65f5c5d57caf63")?;
  /// let msg2 = Bytes::copy_from_slice(b"hey\n");
  /// let fp2: hashing::Fingerprint =
  ///   hashing::Fingerprint::from_str("4e955fea0268518cbaa500409dfbec88f0ecebad28d84ecbe250baed97dba889")?;
  ///
  /// byte_store.store_small_bytes_batch(vec![(fp, msg.clone()), (fp2, msg2.clone())], true).await?;
  ///
  /// let digest = hashing::Digest { hash: fp, size_bytes: 4 };
  /// let digest2 = hashing::Digest { hash: fp2, size_bytes: 4 };
  ///
  /// let digest_trie = fs::DigestTrie::from_unique_paths(
  ///   vec![
  ///     fs::directory::TypedPath::File { path: Path::new("a/a.txt"), is_executable: false },
  ///     fs::directory::TypedPath::File { path: Path::new("a/b.txt"), is_executable: false },
  ///   ],
  ///   &vec![
  ///     (PathBuf::from("a/a.txt"), digest),
  ///     (PathBuf::from("a/b.txt"), digest2),
  ///   ].into_iter().collect(),
  /// )?;
  ///
  /// let dir_digest = dir_store.record_digest_trie(digest_trie, true).await?;
  /// assert_eq!(4, dir_digest.digests().len());
  /// assert_eq!(digest, dir_digest.digests()[2]);
  /// assert_eq!(digest2, dir_digest.digests()[1]);
  ///
  /// assert_eq!(dir_digest.as_digest(), hashing::Digest {
  ///   hash: hashing::Fingerprint::from_str("581505c0f50749195a505572dcfff89cd6a4218e61e1bb24496568d1b0362eff")?,
  ///   size_bytes: 76,
  /// });
  /// # Ok(())
  /// # })}
  ///```
  pub async fn record_digest_trie(
    &self,
    tree: fs::DigestTrie,
    initial_lease: bool,
  ) -> Result<fs::DirectoryDigest, HandleError> {
    Ok(self.store().record_digest_trie(tree, initial_lease).await?)
  }

  /// Expand the symbolic directory tree associated with `digest`.
  ///
  ///```
  /// # use executor_resource_handles::HandleError;
  /// # fn main() -> Result<(), HandleError> { tokio_test::block_on(async {
  /// use executor_resource_handles::Handles;
  /// use bytes::Bytes;
  /// use tempfile::tempdir;
  /// use std::{path::{Path, PathBuf}, str::FromStr};
  ///
  /// let store_td = tempdir()?;
  /// let handles = Handles::new(store_td.path())?;
  /// let byte_store = handles.byte_store();
  /// let dir_store = handles.directory_store();
  ///
  /// let msg = Bytes::copy_from_slice(b"wow\n");
  /// let fp: hashing::Fingerprint =
  ///   hashing::Fingerprint::from_str("f40cd21f276e47d533371afce1778447e858eb5c9c0c0ed61c65f5c5d57caf63")?;
  /// let msg2 = Bytes::copy_from_slice(b"hey\n");
  /// let fp2: hashing::Fingerprint =
  ///   hashing::Fingerprint::from_str("4e955fea0268518cbaa500409dfbec88f0ecebad28d84ecbe250baed97dba889")?;
  ///
  /// byte_store.store_small_bytes_batch(vec![(fp, msg.clone()), (fp2, msg2.clone())], true).await?;
  ///
  /// let digest = hashing::Digest { hash: fp, size_bytes: 4 };
  /// let digest2 = hashing::Digest { hash: fp2, size_bytes: 4 };
  ///
  /// let digest_trie = fs::DigestTrie::from_unique_paths(
  ///   vec![
  ///     fs::directory::TypedPath::File { path: Path::new("a/a.txt"), is_executable: false },
  ///     fs::directory::TypedPath::File { path: Path::new("a/b.txt"), is_executable: false },
  ///   ],
  ///   &vec![
  ///     (PathBuf::from("a/a.txt"), digest),
  ///     (PathBuf::from("a/b.txt"), digest2),
  ///   ].into_iter().collect(),
  /// )?;
  ///
  /// let dir_digest = dir_store.record_digest_trie(digest_trie.clone(), true).await?;
  /// assert_eq!(
  ///   digest_trie.as_remexec_directory(),
  ///   dir_store.load_digest_trie(dir_digest).await?.as_remexec_directory(),
  /// );
  /// # Ok(())
  /// # })}
  ///```
  pub async fn load_digest_trie(
    &self,
    digest: fs::DirectoryDigest,
  ) -> Result<fs::DigestTrie, HandleError> {
    Ok(self.store().load_digest_trie(digest).await?)
  }

  /// Extract a directory structure from the given `digest`.
  ///
  ///```
  /// # use executor_resource_handles::HandleError;
  /// # fn main() -> Result<(), HandleError> { tokio_test::block_on(async {
  /// use executor_resource_handles::Handles;
  /// use bytes::Bytes;
  /// use tempfile::tempdir;
  /// use std::{path::{Path, PathBuf}, str::FromStr};
  ///
  /// let store_td = tempdir()?;
  /// let handles = Handles::new(store_td.path())?;
  /// let byte_store = handles.byte_store();
  /// let dir_store = handles.directory_store();
  ///
  /// let msg = Bytes::copy_from_slice(b"wow\n");
  /// let fp: hashing::Fingerprint =
  ///   hashing::Fingerprint::from_str("f40cd21f276e47d533371afce1778447e858eb5c9c0c0ed61c65f5c5d57caf63")?;
  /// let msg2 = Bytes::copy_from_slice(b"hey\n");
  /// let fp2: hashing::Fingerprint =
  ///   hashing::Fingerprint::from_str("4e955fea0268518cbaa500409dfbec88f0ecebad28d84ecbe250baed97dba889")?;
  ///
  /// byte_store.store_small_bytes_batch(vec![(fp, msg.clone()), (fp2, msg2.clone())], true).await?;
  ///
  /// let digest = hashing::Digest { hash: fp, size_bytes: 4 };
  /// let digest2 = hashing::Digest { hash: fp2, size_bytes: 4 };
  ///
  /// let digest_trie = fs::DigestTrie::from_unique_paths(
  ///   vec![
  ///     fs::directory::TypedPath::File { path: Path::new("a/a.txt"), is_executable: false },
  ///     fs::directory::TypedPath::File { path: Path::new("a/b.txt"), is_executable: false },
  ///   ],
  ///   &vec![
  ///     (PathBuf::from("a/a.txt"), digest),
  ///     (PathBuf::from("a/b.txt"), digest2),
  ///   ].into_iter().collect(),
  /// )?;
  ///
  /// let dir_digest = dir_store.record_digest_trie(digest_trie.clone(), true).await?;
  /// assert_eq!(dir_digest, dir_store.load_directory_digest(dir_digest.as_digest()).await?);
  /// # Ok(())
  /// # })}
  ///```
  pub async fn load_directory_digest(
    &self,
    digest: hashing::Digest,
  ) -> Result<fs::DirectoryDigest, HandleError> {
    Ok(self.store().load_directory_digest(digest).await?)
  }

  /// Generate a snapshot by enumerating file paths from `digest`.
  ///
  ///```
  /// # use executor_resource_handles::HandleError;
  /// # fn main() -> Result<(), HandleError> { tokio_test::block_on(async {
  /// use executor_resource_handles::Handles;
  /// use bytes::Bytes;
  /// use tempfile::tempdir;
  /// use std::{path::{Path, PathBuf}, str::FromStr};
  ///
  /// let store_td = tempdir()?;
  /// let handles = Handles::new(store_td.path())?;
  /// let byte_store = handles.byte_store();
  /// let dir_store = handles.directory_store();
  ///
  /// let msg = Bytes::copy_from_slice(b"wow\n");
  /// let fp: hashing::Fingerprint =
  ///   hashing::Fingerprint::from_str("f40cd21f276e47d533371afce1778447e858eb5c9c0c0ed61c65f5c5d57caf63")?;
  /// let msg2 = Bytes::copy_from_slice(b"hey\n");
  /// let fp2: hashing::Fingerprint =
  ///   hashing::Fingerprint::from_str("4e955fea0268518cbaa500409dfbec88f0ecebad28d84ecbe250baed97dba889")?;
  ///
  /// byte_store.store_small_bytes_batch(vec![(fp, msg.clone()), (fp2, msg2.clone())], true).await?;
  ///
  /// let digest = hashing::Digest { hash: fp, size_bytes: 4 };
  /// let digest2 = hashing::Digest { hash: fp2, size_bytes: 4 };
  ///
  /// let digest_trie = fs::DigestTrie::from_unique_paths(
  ///   vec![
  ///     fs::directory::TypedPath::File { path: Path::new("a/a.txt"), is_executable: false },
  ///     fs::directory::TypedPath::File { path: Path::new("a/b.txt"), is_executable: false },
  ///   ],
  ///   &vec![
  ///     (PathBuf::from("a/a.txt"), digest),
  ///     (PathBuf::from("a/b.txt"), digest2),
  ///   ].into_iter().collect(),
  /// )?;
  ///
  /// let dir_digest = dir_store.record_digest_trie(digest_trie.clone(), true).await?;
  /// let snapshot = dir_store.load_snapshot(dir_digest).await?;
  /// assert_eq!(snapshot.files(), vec![PathBuf::from("a/a.txt"), PathBuf::from("a/b.txt")]);
  /// assert_eq!(snapshot.directories(), vec![PathBuf::from("a")]);
  /// # Ok(())
  /// # })}
  ///```
  pub async fn load_snapshot(
    &self,
    digest: fs::DirectoryDigest,
  ) -> Result<store::Snapshot, HandleError> {
    Ok(store::Snapshot::from_digest(self.store().clone(), digest).await?)
  }
}

/// Manipulate zip files as directory trees.
#[derive(Debug, Clone)]
pub struct ZipFileStore {
  byte_store: ByteStore,
  directory_store: DirectoryStore,
}

impl ZipFileStore {
  pub(crate) fn new(byte_store: ByteStore, directory_store: DirectoryStore) -> Self {
    Self {
      byte_store,
      directory_store,
    }
  }

  /// Enter the contents of `archive` into the local store as a directory tree.
  ///
  ///```
  /// # use executor_resource_handles::HandleError;
  /// # fn main() -> Result<(), HandleError> { tokio_test::block_on(async {
  /// use executor_resource_handles::Handles;
  /// use tempfile::tempdir;
  /// use zip::{ZipArchive, ZipWriter, write::FileOptions};
  /// use std::{io::{Write, Cursor}, str::FromStr};
  ///
  /// let store_td = tempdir()?;
  /// let handles = Handles::new(store_td.path())?;
  /// let zip_store = handles.zip_file_store();
  ///
  /// let buf = Cursor::new(Vec::new());
  /// let mut zip = ZipWriter::new(buf);
  /// let options = FileOptions::default();
  /// zip.start_file("hello_world.txt", options)?;
  /// zip.write(b"Hello, World!")?;
  /// let buf = zip.finish()?;
  ///
  /// let zip = ZipArchive::new(buf)?;
  /// let dir_digest = zip_store.store_zip_entries(zip).await?;
  /// let digest = hashing::Digest {
  ///   hash: hashing::Fingerprint::from_str("a44a45978694cc8277b5df3ca4de74e7a6c3a8230e714937b96f3a0bcee68308")?,
  ///   size_bytes: 89,
  /// };
  /// assert_eq!(digest, dir_digest.as_digest());
  /// # Ok(())
  /// # })}
  ///```
  pub async fn store_zip_entries<R: io::Read + io::Seek + Send>(
    &self,
    archive: zip::ZipArchive<R>,
  ) -> Result<fs::DirectoryDigest, HandleError> {
    let mut path_stats: Vec<fs::PathStat> = Vec::new();
    let mut file_digests: HashMap<PathBuf, hashing::Digest> = HashMap::new();

    let archive = ZipReader::new(archive);
    let s = archive.stream_entries().await;
    pin_mut!(s);

    while let Some(ret) = s.next().await {
      let (stat, tmp_path) = ret?;
      if let Some(tmp_path) = tmp_path {
        let digest = self
          .byte_store
          .store_streaming_file(true, true, tmp_path.to_path_buf())
          .await?;
        /* NB: We ignore duplicate filenames, although their contents are always still entered. */
        let _ = file_digests.insert(stat.path().to_path_buf(), digest);
      }
      path_stats.push(stat);
    }

    let path_stats = path_stats
      .iter()
      .map(fs::directory::TypedPath::from)
      .collect();
    let digest_trie = fs::directory::DigestTrie::from_unique_paths(path_stats, &file_digests)?;
    self
      .directory_store
      .record_digest_trie(digest_trie, true)
      .await
  }

  /// Extract the contents of `digest` into `archive`.
  ///
  ///```
  /// # use executor_resource_handles::HandleError;
  /// # fn main() -> Result<(), HandleError> { tokio_test::block_on(async {
  /// use executor_resource_handles::Handles;
  /// use tempfile::tempdir;
  /// use zip::{ZipArchive, ZipWriter, write::FileOptions};
  /// use std::{io::{Write, Cursor}, str::FromStr};
  ///
  /// let store_td = tempdir()?;
  /// let handles = Handles::new(store_td.path())?;
  /// let zip_store = handles.zip_file_store();
  ///
  /// let buf = Cursor::new(Vec::new());
  /// let mut zip = ZipWriter::new(buf);
  /// let options = FileOptions::default();
  /// zip.start_file("hello_world.txt", options)?;
  /// zip.write(b"Hello, World!")?;
  /// let buf = zip.finish()?;
  ///
  /// let zip = ZipArchive::new(buf)?;
  /// let dir_digest = zip_store.store_zip_entries(zip).await?;
  /// let digest = hashing::Digest {
  ///   hash: hashing::Fingerprint::from_str("a44a45978694cc8277b5df3ca4de74e7a6c3a8230e714937b96f3a0bcee68308")?,
  ///   size_bytes: 89,
  /// };
  /// assert_eq!(digest, dir_digest.as_digest());
  ///
  /// let buf = Cursor::new(Vec::new());
  /// let zip = ZipWriter::new(buf);
  /// let mut zip = zip_store.synthesize_zip_file(dir_digest, zip).await?;
  /// let buf = zip.finish()?;
  ///
  /// let mut zip = ZipArchive::new(buf)?;
  /// let extract_dir = tempdir()?;
  /// zip.extract(&extract_dir)?;
  /// let ret = std::fs::read_to_string(extract_dir.path().join("hello_world.txt"))?;
  /// assert_eq!(ret, "Hello, World!");
  /// # Ok(())
  /// # })}
  ///```
  pub async fn synthesize_zip_file<W: io::Write + io::Seek + Send>(
    &self,
    digest: fs::DirectoryDigest,
    mut archive: zip::ZipWriter<W>,
  ) -> Result<zip::ZipWriter<W>, HandleError> {
    let digest_trie = self.directory_store.load_digest_trie(digest).await?;

    let mut entries: Vec<(PathBuf, fs::directory::Entry)> = Vec::new();
    digest_trie.walk(
      fs::SymlinkBehavior::Oblivious,
      &mut |path: &Path, entry: &fs::directory::Entry| {
        if path.as_os_str().is_empty() {
          /* Ignore the top-level directory, as that does not have a corresponding entry in the zip
           * file. */
          assert!(matches![entry, fs::directory::Entry::Directory(_)]);
        } else {
          entries.push((path.to_path_buf(), entry.clone()));
        }
      },
    );

    /* FIXME: explicitly use DateTime::zero() when https://github.com/zip-rs/zip/pull/399 is
     * merged! */
    let options = zip::write::FileOptions::default().last_modified_time(DateTime::default());

    for (path, entry) in entries.into_iter() {
      let path = format!("{}", path.display());
      dbg!(&path);
      dbg!(&entry);
      match entry {
        fs::directory::Entry::Directory(_) => {
          archive.add_directory(path, options)?;
        },
        fs::directory::Entry::File(f) => {
          archive.start_file(path, options)?;
          let bytes = self
            .byte_store
            .load_file_bytes_with(f.digest(), Bytes::copy_from_slice)
            .await?;
          archive.write_all(&bytes)?;
        },
        _ => unreachable!("we chose SymlinkBehavior::Oblivious!"),
      }
    }

    Ok(archive)
  }
}

/// Execution and storage context.
#[derive(Debug, Clone)]
pub struct Handles {
  executor: Executor,
  fs_store: store::Store,
}

impl Handles {
  /// Create a new executor and local store.
  ///
  /// NB: although it's synchronous, this method must be run from within an active tokio
  /// execution context!
  ///
  ///```
  /// # use executor_resource_handles::HandleError;
  /// # fn main() -> Result<(), HandleError> {
  /// use executor_resource_handles::Handles;
  /// use tempfile::tempdir;
  ///
  /// let local_store_path = tempdir()?;
  /// let _ = tokio_test::block_on(async { Handles::new(local_store_path.path()) })?;
  /// # Ok(())
  /// # }
  ///```
  pub fn new(local_store_path: impl AsRef<Path>) -> Result<Self, HandleError> {
    let exe = Executor::new();
    let local_store = store::Store::local_only(exe.clone(), local_store_path)?;
    Ok(Self {
      executor: exe,
      fs_store: local_store,
    })
  }

  /// Produce a crawler using the resources owned by this handle.
  pub fn crawler(&self) -> Crawler {
    Crawler::new(self.clone())
  }

  /// Produce a byte store using the resources owned by this handle.
  pub fn byte_store(&self) -> ByteStore {
    ByteStore::new(self.clone())
  }

  /// Produce a directory store using the resources owned by this handle.
  pub fn directory_store(&self) -> DirectoryStore {
    DirectoryStore::new(self.byte_store())
  }

  /// Produce a zip file store using the resources owned by this handle.
  pub fn zip_file_store(&self) -> ZipFileStore {
    ZipFileStore::new(self.byte_store(), self.directory_store())
  }
}

impl HasExecutor for Handles {
  fn executor(&self) -> &Executor {
    &self.executor
  }
}

impl HasStore for Handles {
  fn store(&self) -> &store::Store {
    &self.fs_store
  }
}

struct ZipReader<R> {
  inner: usize,
  num_entries: usize,
  _ph: PhantomData<R>,
}

impl<'a, R: io::Read + io::Seek + 'a> ZipReader<R> {
  fn process_zip_file(
    &self,
    index: usize,
  ) -> Result<(fs::PathStat, Option<Reader<zip::read::ZipFile<'a>>>), HandleError> {
    let inner: Box<zip::ZipArchive<R>> =
      unsafe { Box::from_raw(self.inner as *mut zip::ZipArchive<R>) };
    let zf = Box::leak::<'a>(inner).by_index(index)?;

    let path: PathBuf = if let Some(name) = zf.enclosed_name() {
      Ok(name.to_path_buf())
    } else {
      Err(PathTransformError::MalformedZipName(zf.name().to_string()))
    }?;

    let ret = if zf.is_dir() {
      let stat = fs::PathStat::Dir {
        path: path.clone(),
        stat: fs::Dir(path),
      };
      (stat, None)
    } else {
      let is_executable = zf.unix_mode().map(|m| (m & 0o111) != 0).unwrap_or(false);
      let stat = fs::PathStat::File {
        path: path.clone(),
        stat: fs::File {
          path,
          is_executable,
        },
      };

      (stat, Some(Reader::new(zf)))
    };
    Ok(ret)
  }
}

impl<R> ops::Drop for ZipReader<R> {
  fn drop(&mut self) {
    /* Drop the box. */
    let _: Box<zip::ZipArchive<R>> =
      unsafe { Box::from_raw(self.inner as *mut zip::ZipArchive<R>) };
  }
}

impl<R: io::Read + io::Seek> ZipReader<R> {
  pub fn new(inner: zip::ZipArchive<R>) -> Self {
    let num_entries = inner.len();
    Self {
      inner: Box::into_raw(Box::new(inner)) as usize,
      num_entries,
      _ph: PhantomData,
    }
  }

  pub async fn stream_entries(
    self,
  ) -> impl Stream<Item = Result<(fs::PathStat, Option<tempfile::TempPath>), HandleError>> {
    try_stream! {
      for i in 0..self.num_entries {
        let (stat, reader) = self.process_zip_file(i)?;
        let tmp_path = if let Some(reader) = reader {
          Some(reader.expand_reader().await?)
        } else {
          None
        };
        let ret = (stat, tmp_path);
        yield ret;
      }
    }
  }
}

#[cfg(test)]
mod test {
  use super::*;

  #[tokio::test]
  async fn zip_round_trip() -> Result<(), HandleError> {
    let store_td = tempfile::tempdir()?;
    let handles = Handles::new(store_td.path())?;
    let crawler = handles.crawler();
    let dir_store = handles.directory_store();
    let zip_store = handles.zip_file_store();

    let td = tempfile::tempdir()?;
    std::fs::write(td.path().join("a.txt"), "wow\n")?;
    std::fs::write(td.path().join("b.txt"), "hey\n")?;
    std::fs::create_dir(td.path().join("c"))?;
    std::fs::write(td.path().join("c/d.txt"), "oh\n")?;
    std::fs::create_dir(td.path().join("c/e"))?;
    std::fs::write(td.path().join("c/e/f.txt"), "yeah\n")?;

    let buf = io::Cursor::new(Vec::new());
    let mut zip = zip::ZipWriter::new(buf);
    /* FIXME: explicitly use DateTime::zero() when https://github.com/zip-rs/zip/pull/399 is
     * merged! */
    let options = zip::write::FileOptions::default().last_modified_time(DateTime::default());
    zip.start_file("a.txt", options)?;
    io::copy(
      &mut std::fs::OpenOptions::new()
        .read(true)
        .open(td.path().join("a.txt"))?,
      &mut zip,
    )?;
    zip.start_file("b.txt", options)?;
    io::copy(
      &mut std::fs::OpenOptions::new()
        .read(true)
        .open(td.path().join("b.txt"))?,
      &mut zip,
    )?;
    zip.add_directory("c", options)?;
    zip.start_file("c/d.txt", options)?;
    io::copy(
      &mut std::fs::OpenOptions::new()
        .read(true)
        .open(td.path().join("c/d.txt"))?,
      &mut zip,
    )?;
    zip.add_directory("c/e", options)?;
    zip.start_file("c/e/f.txt", options)?;
    io::copy(
      &mut std::fs::OpenOptions::new()
        .read(true)
        .open(td.path().join("c/e/f.txt"))?,
      &mut zip,
    )?;
    let buf = zip.finish()?;
    let zip = zip::ZipArchive::new(buf)?;
    let manual_zip_dir_digest = zip_store.store_zip_entries(zip).await?;
    let manual_zip_snapshot = dir_store
      .load_snapshot(manual_zip_dir_digest.clone())
      .await?;

    let globs = fs::PreparedPathGlobs::create(
      vec!["**/*.txt".to_string()],
      fs::StrictGlobMatching::Ignore,
      fs::GlobExpansionConjunction::AnyMatch,
    )?;
    /* Assert that the manually generated zip file produces the same checksums as crawling the
     * directory. */
    let snapshot = crawler.expand_globs(td.path(), globs.clone()).await?;
    assert_eq!(manual_zip_snapshot, snapshot);
    let dir_digest: fs::DirectoryDigest = snapshot.clone().into();
    assert_eq!(dir_digest, manual_zip_dir_digest);

    let extract_td = tempfile::tempdir()?;
    let buf = io::Cursor::new(Vec::new());
    let zip = zip::ZipWriter::new(buf);
    let mut zip = zip_store
      .synthesize_zip_file(dir_digest.clone(), zip)
      .await?;
    let buf = zip.finish()?;
    let mut zip = zip::ZipArchive::new(buf)?;
    zip.extract(&extract_td)?;
    /* Assert that crawling the directory after extracting the synthesized zip file produces the
     * same checksums. */
    let extracted_snapshot = crawler.expand_globs(extract_td.path(), globs).await?;
    assert_eq!(extracted_snapshot, snapshot);
    let extracted_dir_digest: fs::DirectoryDigest = extracted_snapshot.into();
    assert_eq!(extracted_dir_digest, dir_digest);

    Ok(())
  }
}
