// use std::fmt::{Display, Formatter};
//
// use crate::key::{Key, Scope, Segment};
//
// #[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
// pub struct Namespaced<'a, T> {
//     namespace: Segment,
//     value: &'a T,
// }
//
// impl Display for Namespaced<'_, Key> {
//     fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
//         write!(f, "{}{}{}", self.namespace, Scope::SEPARATOR, self.value)
//     }
// }
//
// impl Display for Namespaced<'_, Scope> {
//     fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
//         write!(f, "{}{}{}", self.namespace, Scope::SEPARATOR, self.value)
//     }
// }
