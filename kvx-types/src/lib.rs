pub use key::Key;
pub use namespace::{Namespace, NamespaceBuf, ParseNamespaceError};
pub use scope::Scope;
pub use segment::{ParseSegmentError, Segment, SegmentBuf};

mod key;
mod namespace;
mod scope;
mod segment;
