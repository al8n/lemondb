const MANIFEST_DELETIONS_REWRITE_THRESHOLD: usize = 10000;

/// The options for opening a manifest file.
#[viewit::viewit(getters(style = "move"), setters(prefix = "with"))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ManifestOptions {
  /// The version of the lemon manifest file. Default is `0`.
  #[viewit(
    getter(const, attrs(doc = "Returns the version of the manifest file.")),
    setter(attrs(doc = "Sets the version of the manifest file."))
  )]
  version: u16,
  /// The rewrite threshold for the manifest file. Default is `10000`.
  #[viewit(
    getter(
      const,
      attrs(doc = "Returns the rewrite threshold for the manifest file.")
    ),
    setter(attrs(doc = "Sets the rewrite threshold for the manifest file."))
  )]
  rewrite_threshold: usize,
}

impl Default for ManifestOptions {
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

impl ManifestOptions {
  /// Creates a new manifest options with the default values.
  #[inline]
  pub const fn new() -> Self {
    Self {
      version: 0,
      rewrite_threshold: MANIFEST_DELETIONS_REWRITE_THRESHOLD,
    }
  }
}
