use std::sync::Arc;

use rustpipe::{Next, Pipe, PipeResult, Pipeline};

#[derive(Debug)]
struct ActixLikeServiceRequest {
    path: String,
    extensions: Vec<String>,
}

struct SessionLoader;

impl Pipe<ActixLikeServiceRequest> for SessionLoader {
    fn handle(
        &self,
        mut passable: ActixLikeServiceRequest,
        next: Next<'_, ActixLikeServiceRequest>,
    ) -> PipeResult<ActixLikeServiceRequest> {
        passable.extensions.push("session:user-42".to_string());
        next.handle(passable)
    }
}

struct CsrfGuard;

impl Pipe<ActixLikeServiceRequest> for CsrfGuard {
    fn handle(
        &self,
        mut passable: ActixLikeServiceRequest,
        next: Next<'_, ActixLikeServiceRequest>,
    ) -> PipeResult<ActixLikeServiceRequest> {
        passable.extensions.push("csrf:verified".to_string());
        next.handle(passable)
    }
}

fn main() -> rustpipe::PipelineResult<()> {
    let request = ActixLikeServiceRequest {
        path: "/settings".to_string(),
        extensions: Vec::new(),
    };

    let request = Pipeline::new()
        .send(request)
        .through(vec![Arc::new(SessionLoader), Arc::new(CsrfGuard)])
        .then_return()?;

    println!("{} {:?}", request.path, request.extensions);
    Ok(())
}
