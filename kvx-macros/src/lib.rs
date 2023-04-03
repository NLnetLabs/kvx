use kvx::Scope;
use proc_macro::TokenStream;
use proc_macro_error::{abort, proc_macro_error};
use quote::quote;
use syn::{parse_macro_input, LitStr};

#[proc_macro]
#[proc_macro_error]
pub fn segment(input: TokenStream) -> TokenStream {
    let s = parse_macro_input!(input as LitStr).value();
    if s.trim() != s {
        abort! { s,
            "invalid Segment";
                note = "Segment may not contain trailing white space";
        }
    } else if s.is_empty() {
        abort! { s,
            "invalid Segment";
                note = "Segment may not be empty";
        }
    } else if s.contains(Scope::SEPARATOR) {
        abort! { s,
            "invalid Segment";
                note = format!("Segment may not contain {}", Scope::SEPARATOR);
        }
    } else {
        quote!(::kvx::Segment::new(#s).unwrap()).into()
    }
}
