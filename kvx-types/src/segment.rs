use std::{
    borrow::Borrow,
    fmt::{Display, Formatter},
    ops::Deref,
    str::FromStr,
};

use thiserror::Error;

use crate::Scope;

/// A nonempty string that does not start or end with whitespace and does not
/// contain any instances of [`Scope::SEPARATOR`].
///
/// This is the owned variant of [`Segment`].
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[repr(transparent)]
pub struct SegmentBuf(String);

impl AsRef<Segment> for SegmentBuf {
    fn as_ref(&self) -> &Segment {
        self
    }
}

impl Borrow<Segment> for SegmentBuf {
    fn borrow(&self) -> &Segment {
        self
    }
}

impl Deref for SegmentBuf {
    type Target = Segment;

    fn deref(&self) -> &Self::Target {
        unsafe { Segment::from_str_unchecked(&self.0) }
    }
}

impl Display for SegmentBuf {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for SegmentBuf {
    type Err = ParseSegmentError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Segment::parse(s)?.to_owned())
    }
}

impl From<&Segment> for SegmentBuf {
    fn from(value: &Segment) -> Self {
        value.to_owned()
    }
}

/// A nonempty string slice that does not start or end with whitespace and does
/// not contain any instances of [`Scope::SEPARATOR`].
///
/// For the owned variant, see [`SegmentBuf`].
#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[repr(transparent)]
pub struct Segment(str);

impl Segment {
    /// Parse a Segment from a string.
    ///
    /// # Examples
    /// ```rust
    /// # use kvx_types::ParseSegmentError;
    /// use kvx_types::Segment;
    ///
    /// # fn main() -> Result<(), ParseSegmentError> {
    /// Segment::parse("segment")?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    /// If the string is empty, starts or ends with whitespace, or contains a
    /// [`Scope::SEPARATOR`] a [`ParseSegmentError`] variant will be returned.
    pub const fn parse(value: &str) -> Result<&Self, ParseSegmentError> {
        if value.is_empty() {
            Err(ParseSegmentError::Empty)
        } else {
            let bytes = value.as_bytes();
            if Self::leading_whitespace(bytes) || Self::trailing_whitespace(bytes) {
                Err(ParseSegmentError::TrailingWhitespace)
            } else if Self::contains_separator(bytes) {
                Err(ParseSegmentError::ContainsSeparator)
            } else {
                unsafe { Ok(Segment::from_str_unchecked(value)) }
            }
        }
    }

    /// Return the encapsulated string.
    ///
    /// # Examples
    /// ```rust
    /// # use kvx_types::ParseSegmentError;
    /// use kvx_types::Segment;
    ///
    /// # fn main() -> Result<(), ParseSegmentError> {
    /// let segment_str = "segment";
    /// let segment = Segment::parse(segment_str)?;
    /// assert_eq!(segment.as_str(), segment_str);
    /// # Ok(())
    /// # }
    /// ```
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Creates a Segment from a string without performing any checks.
    ///
    /// # Safety
    /// This function should only be called from the [`kvx_macros`] crate - do
    /// not use directly.
    ///
    /// [`kvx_macros`]: ../kvx_macros/index.html
    pub const unsafe fn from_str_unchecked(s: &str) -> &Self {
        &*(s as *const _ as *const Self)
    }

    const fn leading_whitespace(bytes: &[u8]) -> bool {
        matches!(bytes[0], 9 | 10 | 32)
    }

    const fn trailing_whitespace(bytes: &[u8]) -> bool {
        matches!(bytes[bytes.len() - 1], 9 | 10 | 32)
    }

    const fn contains_separator(bytes: &[u8]) -> bool {
        let mut index = 0;

        while index < bytes.len() {
            if bytes[index] == Scope::SEPARATOR as u8 {
                return true;
            }
            index += 1;
        }

        false
    }
}

impl Display for Segment {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.0)
    }
}

impl ToOwned for Segment {
    type Owned = SegmentBuf;

    fn to_owned(&self) -> Self::Owned {
        SegmentBuf(self.0.to_owned())
    }
}

/// Represents all ways parsing a string as a [`Segment`] can fail.
#[derive(Debug, Error)]
pub enum ParseSegmentError {
    #[error("segments must not start or end with whitespace")]
    TrailingWhitespace,
    #[error("segments must be nonempty")]
    Empty,
    #[error("segments must not contain scope separators")]
    ContainsSeparator,
}

#[cfg(feature = "postgres")]
mod postgres_impls {
    use crate::segment::{Segment, SegmentBuf};

    impl postgres::types::ToSql for &Segment {
        fn to_sql(
            &self,
            ty: &postgres_types::Type,
            out: &mut postgres_types::private::BytesMut,
        ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>>
        where
            Self: Sized,
        {
            (&self.0).to_sql(ty, out)
        }

        fn accepts(ty: &postgres_types::Type) -> bool
        where
            Self: Sized,
        {
            <&str>::accepts(ty)
        }

        fn to_sql_checked(
            &self,
            ty: &postgres_types::Type,
            out: &mut postgres_types::private::BytesMut,
        ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
            (&self.0).to_sql_checked(ty, out)
        }
    }

    impl postgres::types::ToSql for SegmentBuf {
        fn to_sql(
            &self,
            ty: &postgres_types::Type,
            out: &mut postgres_types::private::BytesMut,
        ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>>
        where
            Self: Sized,
        {
            self.0.to_sql(ty, out)
        }

        fn accepts(ty: &postgres_types::Type) -> bool
        where
            Self: Sized,
        {
            String::accepts(ty)
        }

        fn to_sql_checked(
            &self,
            ty: &postgres_types::Type,
            out: &mut postgres_types::private::BytesMut,
        ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
            self.0.to_sql_checked(ty, out)
        }
    }

    impl<'a> postgres::types::FromSql<'a> for SegmentBuf {
        fn from_sql(
            ty: &postgres_types::Type,
            raw: &'a [u8],
        ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
            let value = String::from_sql(ty, raw)?;
            Ok(Segment::parse(&value)?.to_owned())
        }

        fn accepts(ty: &postgres_types::Type) -> bool {
            String::accepts(ty)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Scope, Segment};

    #[test]
    fn test_trailing_separator_fails() {
        assert!(Segment::parse(&format!("test{}", Scope::SEPARATOR)).is_err());
    }

    #[test]
    fn test_trailing_space_fails() {
        assert!(Segment::parse("test ").is_err());
    }

    #[test]
    fn test_trailing_tab_fails() {
        assert!(Segment::parse("test\t").is_err());
    }

    #[test]
    fn test_trailing_newline_fails() {
        assert!(Segment::parse("test\n").is_err());
    }

    #[test]
    fn test_leading_separator_fails() {
        assert!(Segment::parse(&format!("{}test", Scope::SEPARATOR)).is_err());
    }

    #[test]
    fn test_leading_space_fails() {
        assert!(Segment::parse(" test").is_err());
    }

    #[test]
    fn test_leading_tab_fails() {
        assert!(Segment::parse("\ttest").is_err());
    }

    #[test]
    fn test_leading_newline_fails() {
        assert!(Segment::parse("\ntest").is_err());
    }

    #[test]
    fn test_containing_separator_fails() {
        assert!(Segment::parse(&format!("te{}st", Scope::SEPARATOR)).is_err());
    }

    #[test]
    fn test_containing_space_succeeds() {
        assert!(Segment::parse("te st").is_ok());
    }

    #[test]
    fn test_containing_tab_succeeds() {
        assert!(Segment::parse("te\tst").is_ok());
    }

    #[test]
    fn test_containing_newline_succeeds() {
        assert!(Segment::parse("te\nst").is_ok());
    }

    #[test]
    fn test_segment_succeeds() {
        assert!(Segment::parse("test").is_ok())
    }
}
