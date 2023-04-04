use std::{
    fmt::{Display, Formatter},
    ops::Deref,
    str::FromStr,
};

use thiserror::Error;

/// A nonempty string that does not start or end with whitespace and does not
/// contain any instances of [`Scope::SEPARATOR`].
///
/// This is the owned variant of [`Segment`].
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[repr(transparent)]
pub struct SegmentBuf(String);

impl Deref for SegmentBuf {
    type Target = Segment;

    fn deref(&self) -> &Self::Target {
        unsafe { Segment::from_str_unchecked(&self.0) }
    }
}

impl Display for SegmentBuf {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", *self)
    }
}

impl FromStr for SegmentBuf {
    type Err = ParseSegmentError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Segment::parse(s)?.to_segment_buf())
    }
}

impl From<&Segment> for SegmentBuf {
    fn from(value: &Segment) -> Self {
        value.to_segment_buf()
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
    pub const fn parse(value: &str) -> Result<&Self, ParseSegmentError> {
        if value.is_empty() {
            Err(ParseSegmentError::Empty)
        } else {
            unsafe { Ok(Segment::from_str_unchecked(value)) }
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn to_segment_buf(&self) -> SegmentBuf {
        SegmentBuf(self.0.to_owned())
    }

    pub const unsafe fn from_str_unchecked(s: &str) -> &Self {
        &*(s as *const _ as *const Self)
    }
}

impl Display for Segment {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.0)
    }
}

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
    use crate::{key::segment::SegmentBuf, Segment};

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
            Ok(Segment::parse(&value)?.to_segment_buf())
        }

        fn accepts(ty: &postgres_types::Type) -> bool {
            String::accepts(ty)
        }
    }
}
