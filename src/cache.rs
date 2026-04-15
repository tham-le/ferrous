//! Content-addressed local cache for OPeNDAP fetches.
//!
//! The value proposition of Ferrous is "never fetch the same slice twice".
//! When a CLI invocation produces a constraint URL, we hash the full URL
//! (including the query string) and use that as the cache key. A second
//! invocation with identical arguments then hits the local copy and sends
//! zero bytes over the network.
//!
//! # Layout
//!
//! ```text
//! <root>/                      # default: $FERROUS_CACHE_DIR or ~/.ferrous/cache
//!   objects/
//!     ab/                      # first two hex chars of the key (fan-out)
//!       abcdef…                # remaining 14 hex chars — raw bytes payload
//! ```
//!
//! The two-character fan-out keeps any single directory under a few thousand
//! entries even for heavy users, which matters on filesystems that degrade at
//! high directory sizes.
//!
//! # Key hashing
//!
//! We use `std::hash::DefaultHasher` (SipHash-2-4, 64-bit). That's not
//! cryptographically strong, but this is a local cache with no trust boundary
//! — the worst a collision can do is serve one user their own stale bytes,
//! which they can always clear with `ferrous cache clear` (not wired yet).
//! Pulling in `sha2` / `blake3` for a local cache would be overkill.

use std::fs;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::{Path, PathBuf};

use crate::{Error, Result};

/// Directory name used under the cache root for stored objects.
const OBJECTS_DIR: &str = "objects";

/// Environment variable that overrides the cache root.
pub const CACHE_DIR_ENV: &str = "FERROUS_CACHE_DIR";

/// Path-level cache for HTTP response bodies.
#[derive(Clone, Debug)]
pub struct Cache {
    root: PathBuf,
    enabled: bool,
}

impl Cache {
    /// Create a cache rooted at `path`. The directory is created lazily on
    /// first write.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            enabled: true,
        }
    }

    /// Build a cache at the default location:
    /// * `$FERROUS_CACHE_DIR` if set, else
    /// * `$HOME/.ferrous/cache`.
    ///
    /// Falls back to a disabled cache if neither path can be determined
    /// (very rare, only in restricted environments without `$HOME`).
    pub fn default_location() -> Self {
        if let Ok(dir) = std::env::var(CACHE_DIR_ENV) {
            return Self::new(dir);
        }
        if let Ok(home) = std::env::var("HOME") {
            let mut path = PathBuf::from(home);
            path.push(".ferrous");
            path.push("cache");
            return Self::new(path);
        }
        Self::disabled()
    }

    /// Construct a disabled cache. Every `get` returns `None`, every `put`
    /// silently does nothing. Used by `--no-cache`.
    pub fn disabled() -> Self {
        Self {
            root: PathBuf::new(),
            enabled: false,
        }
    }

    /// `true` if the cache is active.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Cache root directory.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Resolve the on-disk path a cache key maps to.
    pub fn path_for(&self, key: &str) -> PathBuf {
        let hash = hash_key(key);
        let mut path = self.root.join(OBJECTS_DIR);
        path.push(&hash[..2]);
        path.push(&hash[2..]);
        path
    }

    /// Return the cached bytes for `key`, or `None` on miss / disabled cache.
    /// I/O errors other than `NotFound` propagate.
    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        if !self.enabled {
            return Ok(None);
        }
        let path = self.path_for(key);
        match fs::read(&path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(Error::Io(e)),
        }
    }

    /// Store `bytes` under `key`, creating parent directories as needed. A
    /// disabled cache drops the call silently.
    pub fn put(&self, key: &str, bytes: &[u8]) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }
        let path = self.path_for(key);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        // Write through a temp file + rename so a crashed process can't leave
        // a half-written object at the cache-hit path.
        let tmp = path.with_extension("tmp");
        fs::write(&tmp, bytes)?;
        fs::rename(&tmp, &path)?;
        Ok(())
    }
}

impl Default for Cache {
    fn default() -> Self {
        Self::default_location()
    }
}

/// Hex-encoded `DefaultHasher` digest. 16 chars is plenty for a local cache
/// — birthday-bound collision at ~4 billion entries.
fn hash_key(key: &str) -> String {
    let mut h = DefaultHasher::new();
    key.hash(&mut h);
    format!("{:016x}", h.finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tempdir() -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "ferrous-cache-test-{}-{}",
            std::process::id(),
            // Nanosecond-ish uniqueness via a hash of the current time.
            {
                let mut h = DefaultHasher::new();
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
                    .hash(&mut h);
                h.finish()
            }
        ));
        p
    }

    #[test]
    fn disabled_cache_never_hits() {
        let cache = Cache::disabled();
        assert!(!cache.is_enabled());
        assert!(cache.get("any").unwrap().is_none());
        cache.put("any", b"payload").unwrap(); // no-op, must not panic
        assert!(cache.get("any").unwrap().is_none());
    }

    #[test]
    fn roundtrip_stores_and_reads() {
        let root = tempdir();
        let cache = Cache::new(&root);
        let key = "https://example.org/data.nc.dods?tos[0:1:10]";

        assert!(cache.get(key).unwrap().is_none(), "should miss initially");

        cache.put(key, b"hello world").unwrap();
        let got = cache.get(key).unwrap().expect("cache hit expected");
        assert_eq!(got, b"hello world");

        // Clean up
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn path_for_uses_two_char_fanout() {
        let cache = Cache::new("/some/root");
        let p = cache.path_for("anything");
        let components: Vec<_> = p.components().collect();
        // Last two are <fanout>/<rest-of-hash>
        assert!(components.len() >= 2);
        let last = components.last().unwrap().as_os_str().to_string_lossy();
        let second_last = components[components.len() - 2]
            .as_os_str()
            .to_string_lossy();
        assert_eq!(second_last.len(), 2, "fanout dir should be 2 chars");
        assert_eq!(last.len(), 14, "object filename should be 14 chars");
        assert!(p.starts_with("/some/root/objects"));
    }

    #[test]
    fn different_keys_map_to_different_paths() {
        let cache = Cache::new("/r");
        let p1 = cache.path_for("url_a?a");
        let p2 = cache.path_for("url_a?b");
        assert_ne!(p1, p2);
    }

    #[test]
    fn identical_keys_map_to_same_path() {
        let cache = Cache::new("/r");
        let key = "https://example.org/x?var[0:1:5]";
        assert_eq!(cache.path_for(key), cache.path_for(key));
    }

    #[test]
    fn default_location_respects_env_var() {
        let original = std::env::var(CACHE_DIR_ENV).ok();
        // SAFETY: tests within the same process can race; Rust 1.85+ gates
        // set_var behind an unsafe block. We single-thread the env poke.
        unsafe {
            std::env::set_var(CACHE_DIR_ENV, "/tmp/ferrous-env-test-cache");
        }
        let cache = Cache::default_location();
        assert_eq!(cache.root(), Path::new("/tmp/ferrous-env-test-cache"));
        unsafe {
            match original {
                Some(v) => std::env::set_var(CACHE_DIR_ENV, v),
                None => std::env::remove_var(CACHE_DIR_ENV),
            }
        }
    }

    #[test]
    fn put_survives_partial_writes_via_tmp_rename() {
        // Indirect: put twice with different bytes, ensure the final content
        // is the *second* write and not a mangled interleave. The rename
        // approach guarantees atomicity on POSIX filesystems.
        let root = tempdir();
        let cache = Cache::new(&root);
        cache.put("key", b"first").unwrap();
        cache.put("key", b"second").unwrap();
        assert_eq!(cache.get("key").unwrap().unwrap(), b"second");
        let _ = fs::remove_dir_all(&root);
    }
}
