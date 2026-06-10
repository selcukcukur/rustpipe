use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemStruct, Path, punctuated::Punctuated, Token};

/// This macro automatically implements the `Pipe` trait for a struct.
///
/// **Parameters**
/// - The first argument is the type of the passable value (e.g. `String`).
/// - The second argument is the type of the error (e.g. `PipelineError`).
///
/// **Usage**
/// ```rust
/// use rustpipe::{Pipeline, PipelineError, PipelineResult, Pipe};
/// use rustpipe_macros::pipe;
/// use std::sync::Arc;
///
/// #[pipe(String, PipelineError)]
/// struct DebugPipe;
///
/// impl DebugPipe {
///     fn handle(&self, passable: String) -> Result<String, PipelineError> {
///         Ok(format!("[DEBUG] {}", passable))
///     }
/// }
///
/// fn main() {
///     let result: PipelineResult<String> = Pipeline::new()
///         .send("hello".to_string())
///         .through(vec![Arc::new(DebugPipe)])
///         .then_return();
///
///     assert_eq!(result.unwrap(), "[DEBUG] hello");
/// }
/// ```
#[proc_macro_attribute]
pub fn pipe(args: TokenStream, input: TokenStream) -> TokenStream {
    // Parse the struct definition from the input token stream
    let input_struct = parse_macro_input!(input as ItemStruct);
    let name = &input_struct.ident;

    // Parse the macro arguments as a comma-separated list of type paths
    let args = parse_macro_input!(args with Punctuated::<Path, Token![,]>::parse_terminated);

    // Ensure exactly two arguments are provided (PassableType, ErrorType)
    if args.len() != 2 {
        return syn::Error::new_spanned(
            &name, "expected #[pipe(PassableType, ErrorType)]"
        ).to_compile_error().into();
    }

    // Extract the first argument as the passable type
    let passable_ty = &args[0];
    // Extract the second argument as the error type
    let error_ty = &args[1];

    // Generate the expanded code: implement pipe trait for the struct
    let expanded = quote! {
        #input_struct

        impl Pipe<#passable_ty, #error_ty> for #name {
            fn handle(&self, passable: #passable_ty) -> Result<#passable_ty, #error_ty> {
                self.handle(passable)
            }
        }
    };

    // Return the generated code as a TokenStream
    TokenStream::from(expanded)
}
