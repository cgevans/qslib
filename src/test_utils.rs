/// Check whether tiff-collection.eds is available for testing.
///
/// Returns `Some(path)` if the file exists and appears to be a real EDS file
/// (not a Git LFS pointer). Returns `None` otherwise, allowing tests to skip
/// gracefully in environments where git-lfs has not fetched the file.
pub fn tiff_eds_path_if_available() -> Option<std::path::PathBuf> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tiff-collection.eds");
    let metadata = std::fs::metadata(&path).ok()?;
    // LFS pointer files are ~130 bytes; the real EDS is ~4 MB.
    if metadata.len() < 4096 {
        return None;
    }
    Some(path)
}

/// Convenience macro: call at the top of a test function to skip if
/// tiff-collection.eds is not available.
///
/// Usage:
/// ```ignore
/// #[test]
/// fn test_something() {
///     let eds_path = require_tiff_eds!();
///     // ... use eds_path ...
/// }
/// ```
macro_rules! require_tiff_eds {
    () => {
        match crate::test_utils::tiff_eds_path_if_available() {
            Some(p) => p,
            None => {
                eprintln!(
                    "Skipping {}: tiff-collection.eds not available (git-lfs not fetched?)",
                    module_path!()
                );
                return;
            }
        }
    };
}

pub(crate) use require_tiff_eds;
