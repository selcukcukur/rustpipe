use rustpipe::{AsyncPipeline, AsyncPipe};
use std::future::Future;
use std::pin::Pin;

struct DelayStep;
impl AsyncPipe<String, ()> for DelayStep {
    fn handle<'a>(&'a self, input: String) -> Pin<Box<dyn Future<Output = Result<String, ()>> + 'a>> {
        Box::pin(async move {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            Ok(format!("{} (delayed)", input))
        })
    }
}

#[tokio::main]
async fn main() {
    let pipeline = AsyncPipeline::new().add(DelayStep);
    let result = pipeline.execute("hello async rustpipe".to_string()).await.unwrap();
    println!("{}", result);
}
