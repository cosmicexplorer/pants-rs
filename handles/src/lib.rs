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
use futures::future::try_join_all;
use parking_lot::Mutex;
use thiserror::Error;
use tokio::task::JoinError;
use tokio_stream::{self, StreamExt};
use zip_merge::{self as zip, result::ZipError, DateTime};

use std::{
  collections::HashMap,
  io::{self, Read, Write},
  marker::Unpin,
  os::unix::fs::PermissionsExt,
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
  /// zip reror: {0}
  Zip(#[from] ZipError),
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

/// Efficiently traverse the local disk and enter file contents in the persistent byte store.
#[async_trait]
pub trait Crawler {
  /// Crawl the filesystem and enter every matching entry into the local store.
  ///
  ///```
  /// # fn main() -> Result<(), executor_resource_handles::HandleError> { tokio_test::block_on(async {
  /// use executor_resource_handles::{Crawler, Handles};
  /// use tempfile::tempdir;
  /// use std::{path::PathBuf, str::FromStr};
  ///
  /// let store_td = tempdir()?;
  /// let handles = Handles::new(store_td.path())?;
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
  /// let snapshot = handles.expand_globs(td.path(), globs).await?;
  /// assert_eq!(snapshot.files(), vec![PathBuf::from("a.txt"), PathBuf::from("b.txt")]);
  /// let dir_digest: fs::DirectoryDigest = snapshot.into();
  /// let digest = dir_digest.as_digest();
  /// let fp: hashing::Fingerprint =
  ///   hashing::Fingerprint::from_str("ba3e9666bbee9d4890698773b29532302024f40d0ce4764cfe4c4e91f5517be6")?;
  /// assert_eq!(digest, hashing::Digest { hash: fp, size_bytes: 158 });
  /// # Ok(())
  /// # })}
  ///```
  async fn expand_globs(
    &self,
    root: &Path,
    path_globs: fs::PreparedPathGlobs,
  ) -> Result<store::Snapshot, HandleError>;
}

/// Enter and retrieve byte streams to and from the local store.
#[async_trait]
pub trait ByteStore: HasExecutor {
  /// Store a small byte chunk.
  ///
  /// *NB: Use [`Self::store_streaming_file`] to stream in larger files.*
  ///
  ///```
  /// # fn main() -> Result<(), executor_resource_handles::HandleError> { tokio_test::block_on(async {
  /// use executor_resource_handles::{ByteStore, Handles};
  /// use bytes::Bytes;
  /// use tempfile::tempdir;
  /// use std::str::FromStr;
  ///
  /// let store_td = tempdir()?;
  /// let handles = Handles::new(store_td.path())?;
  ///
  /// let msg = Bytes::copy_from_slice(b"wow\n");
  /// let digest = handles.store_small_bytes(msg, true).await?;
  ///
  /// let fp: hashing::Fingerprint =
  ///   hashing::Fingerprint::from_str("f40cd21f276e47d533371afce1778447e858eb5c9c0c0ed61c65f5c5d57caf63")?;
  /// assert_eq!(digest, hashing::Digest { hash: fp, size_bytes: 4 });
  ///
  /// # Ok(())
  /// # })}
  ///```
  async fn store_small_bytes(
    &self,
    bytes: Bytes,
    initial_lease: bool,
  ) -> Result<hashing::Digest, HandleError>;

  /// Store multiple small byte chunks.
  ///
  ///```
  /// # fn main() -> Result<(), executor_resource_handles::HandleError> { tokio_test::block_on(async {
  /// use executor_resource_handles::{ByteStore, Handles};
  /// use bytes::Bytes;
  /// use tempfile::tempdir;
  /// use std::str::FromStr;
  ///
  /// let store_td = tempdir()?;
  /// let handles = Handles::new(store_td.path())?;
  ///
  /// let msg = Bytes::copy_from_slice(b"wow\n");
  /// let fp: hashing::Fingerprint =
  ///   hashing::Fingerprint::from_str("f40cd21f276e47d533371afce1778447e858eb5c9c0c0ed61c65f5c5d57caf63")?;
  /// let msg2 = Bytes::copy_from_slice(b"hey\n");
  /// let fp2: hashing::Fingerprint =
  ///   hashing::Fingerprint::from_str("4e955fea0268518cbaa500409dfbec88f0ecebad28d84ecbe250baed97dba889")?;
  ///
  /// handles.store_small_bytes_batch(vec![(fp, msg.clone()), (fp2, msg2.clone())], true).await?;
  ///
  /// let digest = hashing::Digest { hash: fp, size_bytes: 4 };
  /// let digest2 = hashing::Digest { hash: fp2, size_bytes: 4 };
  ///
  /// assert_eq!(msg, handles.load_file_bytes_with(digest, Bytes::copy_from_slice).await?);
  /// assert_eq!(msg2, handles.load_file_bytes_with(digest2, Bytes::copy_from_slice).await?);
  /// # Ok(())
  /// # })}
  ///```
  async fn store_small_bytes_batch(
    &self,
    items: Vec<(hashing::Fingerprint, Bytes)>,
    initial_lease: bool,
  ) -> Result<(), HandleError>;

  /// Stream in the contents of `src` to the local store.
  ///
  ///```
  /// # fn main() -> Result<(), executor_resource_handles::HandleError> { tokio_test::block_on(async {
  /// use executor_resource_handles::{ByteStore, Handles};
  /// use tempfile::tempdir;
  /// use std::str::FromStr;
  ///
  /// let store_td = tempdir()?;
  /// let handles = Handles::new(store_td.path())?;
  ///
  /// let td = tempdir()?;
  /// let out_path = td.path().join("c.txt");
  /// std::fs::write(&out_path, b"hello!!!\n")?;
  ///
  /// let resulting_digest = handles.store_streaming_file(true, true, out_path).await?;
  ///
  /// let fp = hashing::Fingerprint::from_str("d1d23437b40e63d96c9bdd7458e1a61a72b70910d4f053744303f5165d64cbfd")?;
  /// let digest = hashing::Digest { hash: fp, size_bytes: 9 };
  /// assert_eq!(resulting_digest, digest);
  /// # Ok(())
  /// # })}
  ///```
  async fn store_streaming_file(
    &self,
    initial_lease: bool,
    data_is_immutable: bool,
    src: PathBuf,
  ) -> Result<hashing::Digest, HandleError>;

  /// Remove the entry for `digest` from the local store.
  ///
  ///```
  /// # use executor_resource_handles::HandleError;
  /// # fn main() -> Result<(), executor_resource_handles::HandleError> { tokio_test::block_on(async {
  /// use executor_resource_handles::{ByteStore, Handles};
  /// use bytes::Bytes;
  /// use tempfile::tempdir;
  /// use std::str::FromStr;
  ///
  /// let store_td = tempdir()?;
  /// let handles = Handles::new(store_td.path())?;
  ///
  /// let msg = Bytes::copy_from_slice(b"wow\n");
  /// let digest = handles.store_small_bytes(msg.clone(), true).await?;
  ///
  /// let fp: hashing::Fingerprint =
  ///   hashing::Fingerprint::from_str("f40cd21f276e47d533371afce1778447e858eb5c9c0c0ed61c65f5c5d57caf63")?;
  /// assert_eq!(digest, hashing::Digest { hash: fp, size_bytes: 4 });
  ///
  /// assert_eq!(msg, handles.load_file_bytes_with(digest, Bytes::copy_from_slice).await?);
  ///
  /// assert!(handles.remove_entry(digest).await?);
  ///
  /// match handles.load_file_bytes_with(digest, Bytes::copy_from_slice).await {
  ///   Err(HandleError::Store(store::StoreError::MissingDigest(_, _))) => (),
  ///   _ => unreachable!(),
  /// }
  /// # Ok(())
  /// # })}
  ///```
  async fn remove_entry(&self, digest: hashing::Digest) -> Result<bool, HandleError>;

  /// Extract the byte string corresponding to `digest` from the local store.
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

/* #[async_trait] */
/* pub trait ZipFileStore: ByteStore + DirectoryStore { */
/*   async fn store_zip_entries<R: io::Read + io::Seek>( */
/*     &self, */
/*     archive: zip::ZipArchive<R>, */
/*   ) -> Result<fs::DirectoryDigest, HandleError>; */
/*   async fn synthesize_zip_file<W: io::Write + io::Seek + Send>( */
/*     &self, */
/*     digest: fs::DirectoryDigest, */
/*     archive: zip::ZipWriter<W>, */
/*   ) -> Result<zip::ZipWriter<W>, HandleError>; */
/* } */

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

  /* pub async fn store_byte_stream<R: io::Read + Send + Unpin>( */
  /*   &self, */
  /*   initial_lease: bool, */
  /*   src: R, */
  /* ) -> Result<hashing::Digest, HandleError> { */
  /*   /\* FIXME: implement this without an intermediate temporary file! *\/ */
  /*   use tempfile::NamedTempFile; */

  /*   let tmp_out = NamedTempFile::new()?; */
  /*   let tmp_out_path = tmp_out.path().to_path_buf(); */

  /*   self */
  /*     .executor() */
  /*     .native_spawn_blocking(move || { */
  /*       let src = pin!(src); */
  /*       let tmp_out = pin!(tmp_out); */
  /*       io::copy(&mut *src.get_mut(), &mut *tmp_out.get_mut())?; */
  /*       Ok::<(), io::Error>(()) */
  /*     }) */
  /*     .await??; */

  /*   Ok( */
  /*     self */
  /*       .store_streaming_file(initial_lease, true, tmp_out_path) */
  /*       .await?, */
  /*   ) */
  /* } */
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
      .expand_globs(path_globs, fs::SymlinkBehavior::Aware, None)
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

/* #[async_trait] */
/* impl ZipFileStore for Handles { */
/*   async fn store_zip_entries<R: io::Read + io::Seek>( */
/*     &self, */
/*     mut archive: zip::ZipArchive<R>, */
/*   ) -> Result<fs::DirectoryDigest, HandleError> { */
/*     let zip_files = (0..archive.len()) */
/*       .map(|i| archive.by_index(i)) */
/*       .collect::<Result<Vec<zip::read::ZipFile<'_>>, ZipError>>()?; */

/*     let digest_name_map: Vec<(fs::directory::TypedPath, hashing::Digest)> = */
/*       try_join_all(zip_files.into_iter().map(|zf| async move { */
/*         let path = Path::new(zf.name()); */
/*         let typed_path = if zf.is_dir() { */
/*           fs::directory::TypedPath::Dir(path) */
/*         } else { */
/*           let is_executable = zf.unix_mode().map(|m| (m & 0o111) != 0).unwrap_or(false); */
/*           fs::directory::TypedPath::File { */
/*             path, */
/*             is_executable, */
/*           } */
/*         }; */

/*         let digest = self.store_byte_stream(true, zf.clone()).await?; */

/*         Ok::<_, HandleError>((typed_path, digest)) */
/*       })) */
/*       .await?; */

/*     let mut path_stats: Vec<fs::directory::TypedPath> = Vec::new(); */
/*     let mut file_digests: HashMap<PathBuf, hashing::Digest> = HashMap::new(); */
/*     for (typed_path, digest) in digest_name_map.into_iter() { */
/*       let _ = file_digests.insert((*typed_path).to_path_buf(), digest); */
/*       path_stats.push(typed_path); */
/*     } */
/*     let digest_trie = fs::directory::DigestTrie::from_unique_paths(path_stats, &file_digests)?; */
/*     self.record_digest_trie(digest_trie, true).await */
/*   } */

/*   async fn synthesize_zip_file<W: io::Write + io::Seek + Send>( */
/*     &self, */
/*     digest: fs::DirectoryDigest, */
/*     mut archive: zip::ZipWriter<W>, */
/*   ) -> Result<zip::ZipWriter<W>, HandleError> { */
/*     let digest_trie = self.load_digest_trie(digest).await?; */

/*     let mut entries: Vec<(PathBuf, fs::directory::Entry)> = Vec::new(); */
/*     digest_trie.walk( */
/*       fs::SymlinkBehavior::Oblivious, */
/*       &mut |path: &Path, entry: &fs::directory::Entry| { */
/*         entries.push((path.to_path_buf(), entry.clone())); */
/*       }, */
/*     ); */

/*     let options = zip::write::FileOptions::default().last_modified_time(DateTime::zero()); */

/*     let mut stream = tokio_stream::iter(entries); */
/*     while let Some((path, entry)) = stream.next().await { */
/*       let path = format!("{}", path.display()); */
/*       match entry { */
/*         fs::directory::Entry::Directory(_) => { */
/*           archive.add_directory(path, options)?; */
/*         }, */
/*         fs::directory::Entry::File(f) => { */
/*           archive.start_file(path, options)?; */
/*           let bytes = self */
/*             .load_file_bytes_with(f.digest(), Bytes::copy_from_slice) */
/*             .await?; */
/*           archive.write_all(&bytes)?; */
/*         }, */
/*         _ => unreachable!("we chose SymlinkBehavior::Oblivious!"), */
/*       } */
/*     } */

/*     Ok(archive) */
/*   } */
/* } */

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
