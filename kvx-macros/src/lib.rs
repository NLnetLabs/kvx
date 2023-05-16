use kvx_types::Segment;
use proc_macro::TokenStream;
use proc_macro_error::{abort, proc_macro_error};
use quote::quote;
use syn::{parse_macro_input, LitStr};

#[proc_macro]
#[proc_macro_error]
pub fn segment(input: TokenStream) -> TokenStream {
    let s = parse_macro_input!(input as LitStr);

    match Segment::parse(&s.value()) {
        Ok(_) => quote!(unsafe { Segment::from_str_unchecked(#s) }).into(),
        Err(error) => abort!(s.span(), "{}", error),
    }
}
