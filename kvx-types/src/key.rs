use std::{
    fmt::{Display, Formatter},
    str::FromStr,
};

use crate::{
    scope::Scope,
    segment::{ParseSegmentError, Segment, SegmentBuf},
};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Key {
    scope: Scope,
    name: SegmentBuf,
}

impl Key {
    pub fn new_scoped(scope: Scope, name: impl Into<SegmentBuf>) -> Key {
        Key {
            name: name.into(),
            scope,
        }
    }

    pub fn new_global(name: impl Into<SegmentBuf>) -> Key {
        Key::new_scoped(Scope::default(), name)
    }

    pub fn name(&self) -> &Segment {
        &self.name
    }

    pub fn scope(&self) -> &Scope {
        &self.scope
    }

    pub fn with_sub_scope(&self, sub_scope: impl Into<SegmentBuf>) -> Self {
        let mut clone = self.clone();
        clone.add_sub_scope(sub_scope);
        clone
    }

    pub fn add_sub_scope(&mut self, sub_scope: impl Into<SegmentBuf>) {
        self.scope.add_sub_scope(sub_scope);
    }

    pub fn with_namespace(&self, namespace: impl Into<SegmentBuf>) -> Self {
        let mut clone = self.clone();
        clone.add_namespace(namespace);
        clone
    }

    pub fn add_namespace(&mut self, namespace: impl Into<SegmentBuf>) {
        self.scope.add_namespace(namespace);
    }

    pub fn remove_namespace(&mut self, namespace: impl Into<SegmentBuf>) -> Option<SegmentBuf> {
        self.scope.remove_namespace(namespace)
    }
}

impl Display for Key {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.scope.is_global() {
            write!(f, "{}", self.name)
        } else {
            write!(f, "{}{}{}", self.scope, Scope::SEPARATOR, self.name)
        }
    }
}

impl FromStr for Key {
    type Err = ParseSegmentError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut segments: Vec<SegmentBuf> = s
            .split(Scope::SEPARATOR)
            .map(SegmentBuf::from_str)
            .collect::<Result<_, _>>()?;
        let name = segments.pop().unwrap();
        let scope = Scope::new(segments);

        Ok(Key { name, scope })
    }
}
