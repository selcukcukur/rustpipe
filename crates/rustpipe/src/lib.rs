//! Type-safe middleware and transform pipelines for Rust.
//!
//! **rustpipe** provides two pipeline styles:
//! - [`Pipeline`] for continuation-based middleware that can call `Next`.
//! - [`TransformPipeline`] for direct sequential value transformations.

/// Centralized error definitions and handling utilities.
pub mod errors;

/// Continuation-based middleware pipeline implementation.
pub mod middleware;

/// Sequential transform pipeline implementation.
pub mod transform;

/// Core trait and type definitions used across the crate.
pub mod types;

/// Internal helper functions used by pipeline implementations.
pub mod utility;

pub use crate::errors::*;
pub use crate::middleware::*;
pub use crate::transform::*;
pub use crate::types::*;

#[cfg(feature = "macros")]
pub use rustpipe_macros::{pipe, transform_pipe};
