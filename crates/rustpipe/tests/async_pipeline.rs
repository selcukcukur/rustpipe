#![cfg(feature = "async")]

use std::pin::Pin;
use std::sync::Arc;

use rustpipe::{
    AsyncNext, AsyncPipe, AsyncPipeline, AsyncTransformPipe, AsyncTransformPipeline, PipeResult,
    TransformPipeResult,
};

struct AsyncPrefix(&'static str);

impl AsyncPipe<String> for AsyncPrefix {
    fn handle<'a>(
        &'a self,
        passable: String,
        next: AsyncNext<'a, String>,
    ) -> Pin<Box<dyn std::future::Future<Output = PipeResult<String>> + Send + 'a>> {
        Box::pin(async move { next.handle(format!("{}{}", self.0, passable)).await })
    }
}

struct AsyncUpper;

impl AsyncTransformPipe<String> for AsyncUpper {
    fn handle<'a>(
        &'a self,
        passable: String,
    ) -> Pin<Box<dyn std::future::Future<Output = TransformPipeResult<String>> + Send + 'a>> {
        Box::pin(async move { Ok(passable.to_uppercase()) })
    }
}

#[tokio::test]
async fn async_middleware_pipeline_runs_next_chain() {
    let result = AsyncPipeline::new()
        .send("core".to_string())
        .through(vec![Arc::new(AsyncPrefix("app:"))])
        .then_return()
        .await
        .unwrap();

    assert_eq!(result, "app:core");
}

#[tokio::test]
async fn async_transform_pipeline_runs_transforms() {
    let result = AsyncTransformPipeline::new()
        .send("hello".to_string())
        .through(vec![Arc::new(AsyncUpper)])
        .then_return()
        .await
        .unwrap();

    assert_eq!(result, "HELLO");
}
