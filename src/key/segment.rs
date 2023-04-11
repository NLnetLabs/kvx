use std::{
    fmt::{Display, Formatter},
    str::FromStr,
};

use thiserror::Error;

use crate::Scope;

/// A nonempty string that does not start or end with whitespace and does not
/// contain any instances of [`Scope::SEPARATOR`].
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Segment(String);

impl Segment {
    pub fn parse(value: &str) -> Result<Self, ParseSegmentError> {
        value.parse()
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for Segment {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for Segment {
    type Err = ParseSegmentError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.trim() != s {
            Err(ParseSegmentError::TrailingWhitespace)
        } else if s.is_empty() {
            Err(ParseSegmentError::Empty)
        } else if s.contains(Scope::SEPARATOR) {
            Err(ParseSegmentError::ContainsSeparator)
        } else {
            Ok(Segment(s.to_owned()))
        }
    }
}

#[cfg(feature = "postgres")]
impl postgres::types::ToSql for Segment {
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

#[cfg(feature = "postgres")]
impl<'a> postgres::types::FromSql<'a> for Segment {
    fn from_sql(
        ty: &postgres_types::Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let s = String::from_sql(ty, raw)?;
        Ok(Segment::parse(&s)?)
    }

    fn accepts(ty: &postgres_types::Type) -> bool {
        String::accepts(ty)
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
