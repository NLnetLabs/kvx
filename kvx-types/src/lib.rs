pub use key::Key;
pub use namespace::{ParseNamespaceError, Namespace, NamespaceBuf};
pub use scope::Scope;
pub use segment::{ParseSegmentError, Segment, SegmentBuf};

mod key;
mod namespace;
mod scope;
mod segment;
