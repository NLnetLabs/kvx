use std::{
    cmp,
    fmt::{Display, Formatter},
    str::FromStr,
};

use crate::key::{ParseSegmentError, Segment};

#[derive(Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[cfg_attr(
    feature = "postgres",
    derive(postgres::types::ToSql, postgres::types::FromSql)
)]
pub struct Scope {
    segments: Vec<Segment>,
}

impl Scope {
    pub const SEPARATOR: &'static str = "/";

    pub fn from_segment(segment: Segment) -> Self {
        Scope::new(vec![segment])
    }

    pub fn global() -> Self {
        Scope::new(Vec::new())
    }

    pub fn new(segments: Vec<Segment>) -> Self {
        Scope { segments }
    }

    #[cfg(feature = "postgres")]
    pub(crate) fn as_vec(&self) -> &Vec<Segment> {
        &self.segments
    }

    #[cfg(feature = "postgres")]
    pub(crate) fn len(&self) -> i32 {
        self.segments.len() as i32
    }

    pub fn is_global(&self) -> bool {
        self.segments.is_empty()
    }

    pub fn matches(&self, other: &Self) -> bool {
        let min_len = cmp::min(self.segments.len(), other.segments.len());
        self.segments[0..min_len] == other.segments[0..min_len]
    }

    pub fn starts_with(&self, prefix: &Self) -> bool {
        if prefix.segments.len() <= self.segments.len() {
            self.segments[0..prefix.segments.len()] == prefix.segments
        } else {
            false
        }
    }

    pub fn sub_scopes(&self) -> Vec<Scope> {
        self.segments
            .iter()
            .scan(Scope::default(), |state, segment| {
                state.segments.push(segment.clone());
                Some(state.clone())
            })
            .collect()
    }

    pub fn with_sub_scope(&self, namespace: Segment) -> Self {
        let mut clone = self.clone();
        clone.add_sub_scope(namespace);
        clone
    }

    pub fn add_sub_scope(&mut self, sub_scope: Segment) {
        self.segments.push(sub_scope);
    }

    pub fn with_namespace(&self, namespace: Segment) -> Self {
        let mut clone = self.clone();
        clone.add_namespace(namespace);
        clone
    }

    pub fn add_namespace(&mut self, namespace: Segment) {
        self.segments.insert(0, namespace);
    }

    pub fn remove_namespace(&mut self, namespace: Segment) -> Option<Segment> {
        if *self.segments.get(0)? == namespace {
            Some(self.segments.remove(0))
        } else {
            None
        }
    }
}

impl Display for Scope {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            self.segments
                .iter()
                .map(Segment::as_str)
                .collect::<Vec<_>>()
                .join(Self::SEPARATOR)
        )
    }
}

impl FromStr for Scope {
    type Err = ParseSegmentError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.strip_suffix(Self::SEPARATOR).unwrap_or(s);
        let segments = s
            .split(Self::SEPARATOR)
            .map(Segment::from_str)
            .collect::<Result<_, _>>()?;
        Ok(Scope { segments })
    }
}

impl IntoIterator for Scope {
    type IntoIter = <Vec<Segment> as IntoIterator>::IntoIter;
    type Item = <Vec<Segment> as IntoIterator>::Item;

    fn into_iter(self) -> Self::IntoIter {
        self.segments.into_iter()
    }
}

impl<'a> IntoIterator for &'a Scope {
    type IntoIter = <&'a Vec<Segment> as IntoIterator>::IntoIter;
    type Item = <&'a Vec<Segment> as IntoIterator>::Item;

    fn into_iter(self) -> Self::IntoIter {
        self.segments.iter()
    }
}

impl Extend<Segment> for Scope {
    fn extend<T: IntoIterator<Item = Segment>>(&mut self, iter: T) {
        self.segments.extend(iter.into_iter().map(Into::into))
    }
}

impl FromIterator<Segment> for Scope {
    fn from_iter<T: IntoIterator<Item = Segment>>(iter: T) -> Self {
        let segments = iter.into_iter().collect();
        Scope { segments }
    }
}

impl From<Vec<Segment>> for Scope {
    fn from(segments: Vec<Segment>) -> Self {
        Scope { segments }
    }
}

#[cfg(test)]
mod tests {
    use super::Scope;

    #[test]
    fn test_matches() {
        let full: Scope = format!(
            "this{sep}is{sep}a{sep}beautiful{sep}scope",
            sep = Scope::SEPARATOR
        )
        .parse()
        .unwrap();
        let partial: Scope = format!("this{sep}is{sep}a", sep = Scope::SEPARATOR)
            .parse()
            .unwrap();
        let wrong: Scope = format!("this{sep}is{sep}b", sep = Scope::SEPARATOR)
            .parse()
            .unwrap();

        assert!(full.matches(&partial));
        assert!(partial.matches(&full));
        assert!(!partial.matches(&wrong));
        assert!(!wrong.matches(&partial));
        assert!(!full.matches(&wrong));
        assert!(!wrong.matches(&full));
    }

    #[test]
    fn test_starts_with() {
        let full: Scope = format!(
            "this{sep}is{sep}a{sep}beautiful{sep}scope",
            sep = Scope::SEPARATOR
        )
        .parse()
        .unwrap();
        let partial: Scope = format!("this{sep}is{sep}a", sep = Scope::SEPARATOR)
            .parse()
            .unwrap();
        let wrong: Scope = format!("this{sep}is{sep}b", sep = Scope::SEPARATOR)
            .parse()
            .unwrap();

        assert!(full.starts_with(&partial));
        assert!(!partial.starts_with(&full));
        assert!(!partial.starts_with(&wrong));
        assert!(!wrong.starts_with(&partial));
        assert!(!full.starts_with(&wrong));
        assert!(!wrong.starts_with(&full));
    }
}
