use std::{
    fmt::{Display, Formatter},
    str::FromStr,
};

pub use scope::Scope;
pub use segment::{ParseSegmentError, Segment};

mod namespace;
mod scope;
mod segment;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Key {
    scope: Scope,
    name: Segment,
}

impl Key {
    pub fn new_scoped(scope: Scope, name: Segment) -> Key {
        Key { name, scope }
    }

    pub fn new_global(name: Segment) -> Key {
        Key::new_scoped(Scope::default(), name)
    }

    pub fn name(&self) -> &Segment {
        &self.name
    }

    pub fn scope(&self) -> &Scope {
        &self.scope
    }

    pub fn with_sub_scope(&self, sub_scope: Segment) -> Self {
        let mut clone = self.clone();
        clone.add_sub_scope(sub_scope);
        clone
    }

    pub fn add_sub_scope(&mut self, sub_scope: Segment) {
        self.scope.add_sub_scope(sub_scope);
    }

    pub fn with_namespace(&self, namespace: Segment) -> Self {
        let mut clone = self.clone();
        clone.add_namespace(namespace);
        clone
    }

    pub fn add_namespace(&mut self, namespace: Segment) {
        self.scope.add_namespace(namespace);
    }

    pub fn remove_namespace(&mut self, namespace: Segment) -> Option<Segment> {
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
        let mut segments: Vec<Segment> = s
            .split(Scope::SEPARATOR)
            .map(Segment::from_str)
            .collect::<Result<_, _>>()?;
        let name = segments.pop().unwrap();
        let scope = Scope::new(segments);

        Ok(Key { name, scope })
    }
}
