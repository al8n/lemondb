#[cfg(not(feature = "tokio"))]
use futures::lock::Mutex;

#[cfg(feature = "tokio")]
use tokio::sync::Mutex;
