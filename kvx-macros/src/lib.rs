use kvx_types::{Segment, Namespace};
use proc_macro::TokenStream;
use proc_macro_error::{abort, proc_macro_error};
use quote::quote;
use syn::{parse_macro_input, LitStr};

/// Macro to provide a safe way to create a [`&Segment`] at compile-time.
///
/// [`&Segment`]: ../kvx/struct.Segment.html
#[proc_macro]
#[proc_macro_error]
pub fn segment(input: TokenStream) -> TokenStream {
    let s = parse_macro_input!(input as LitStr);

    match Segment::parse(&s.value()) {
        Ok(_) => quote!(unsafe { Segment::from_str_unchecked(#s) }).into(),
        Err(error) => abort!(s.span(), "{}", error),
    }
}

/// Macro to provide a safe way to create a [`&Namespace`] at compile-time.
///
/// [`&Namespace`]: ../kvx/struct.Namespace.html
#[proc_macro]
#[proc_macro_error]
pub fn namespace(input: TokenStream) -> TokenStream {
    let s = parse_macro_input!(input as LitStr);

    match Namespace::parse(&s.value()) {
        Ok(_) => quote!(unsafe { Namespace::from_str_unchecked(#s) }).into(),
        Err(error) => abort!(s.span(), "{}", error),
    }
}