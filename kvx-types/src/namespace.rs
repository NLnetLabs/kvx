use std::{
    borrow::Borrow,
    fmt::{Display, Formatter},
    ops::Deref,
    str::FromStr,
};

use thiserror::Error;

/// An owned Namespace.
/// 
/// This is the owned variant of [`Namespace`]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[repr(transparent)]
pub struct NamespaceBuf(String);

impl AsRef<Namespace> for NamespaceBuf {
    fn as_ref(&self) -> &Namespace {
        self
    }
}

impl Borrow<Namespace> for NamespaceBuf {
    fn borrow(&self) -> &Namespace {
        self
    }
}

impl Deref for NamespaceBuf {
    type Target = Namespace;

    fn deref(&self) -> &Self::Target {
        unsafe { Namespace::from_str_unchecked(&self.0) }
    }
}

impl Display for NamespaceBuf {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for NamespaceBuf {
    type Err = ParseNamespaceError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Namespace::parse(s)?.to_owned())
    }
}

impl From<&Namespace> for NamespaceBuf {
    fn from(value: &Namespace) -> Self {
        value.to_owned()
    }
}

/// A string slice representing a namespace.
/// 
/// Namespaces are used by KeyValueStore to separate
/// different instances that use a shared storage.
/// 
/// Namespace MUST NOT contain any other characters
/// except a-z A-Z 0-9 - or _.
/// 
/// For the owned variant, see [`NamespaceBuf`]
#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Namespace(str);

impl Namespace {
    /// Parse a Namespace from a string.
    ///
    /// # Examples
    /// ```rust
    /// # use kvx_types::ParseNamespaceError;
    /// use kvx_types::Namespace;
    ///
    /// # fn main() -> Result<(), ParseNamespaceError> {
    /// Namespace::parse("Namespace")?;
    /// # Ok(())
    /// # }
    /// ```
    pub const fn parse(value: &str) -> Result<&Self, ParseNamespaceError> {
        if value.is_empty() {
            Err(ParseNamespaceError::Empty)
        } else if value.len() > 255 {
            Err(ParseNamespaceError::TooLong)
        } else if Self::contains_only_legal_chars(value.as_bytes()){
            unsafe { Ok(Namespace::from_str_unchecked(value))}
        } else {
            Err(ParseNamespaceError::IllegalCharacter)
        }
    }

    /// Return the encapsulated string.
    ///
    /// # Examples
    /// ```rust
    /// # use kvx_types::ParseNamespaceError;
    /// use kvx_types::Namespace;
    ///
    /// # fn main() -> Result<(), ParseNamespaceError> {
    /// let namespace_str = "namespace";
    /// let namespace = Namespace::parse(namespace_str)?;
    /// assert_eq!(namespace.as_str(), namespace_str);
    /// # Ok(())
    /// # }
    /// ```
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Creates a Namespace from a string without performing any checks.
    ///
    /// # Safety
    /// This function should only be called from the [`kvx_macros`] crate - do
    /// not use directly.
    ///
    /// [`kvx_macros`]: ../kvx_macros/index.html
    pub const unsafe fn from_str_unchecked(s: &str) -> &Self {
        &*(s as *const _ as *const Self)
    }

    /// We need a const function for checking the bytes we parse
    const fn contains_only_legal_chars(bytes: &[u8]) -> bool {
        let mut index = 0;

        while index < bytes.len() {
            let b = bytes[index];
            if b.is_ascii_alphanumeric() || b == b'-' || b == b'_' {
                index += 1;
            } else {
                return false;
            }
        }

        true
    }
}

impl Display for Namespace {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.0)
    }
}

impl ToOwned for Namespace {
    type Owned = NamespaceBuf;

    fn to_owned(&self) -> Self::Owned {
        NamespaceBuf(self.0.to_owned())
    }
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum ParseNamespaceError {
    #[error("namespaces must be nonempty")]
    Empty,
    #[error("namespaces must not be longer than 255 characters")]
    TooLong,
    #[error("namespace can only alphanumeric characters and - or _")]
    IllegalCharacter
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_empty_namespace_fails() {
        assert_eq!(Namespace::parse(""), Err(ParseNamespaceError::Empty))
    }
}