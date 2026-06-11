use std::sync::Arc;

use rustpipe::{Next, Pipe, PipeResult, Pipeline, PipelineError, StepFailure};

#[derive(Debug)]
struct Request {
    user_id: Option<u64>,
    path: String,
    status: u16,
}

struct Authenticate;

impl Pipe<Request> for Authenticate {
    fn handle(&self, passable: Request, next: Next<'_, Request>) -> PipeResult<Request> {
        if passable.user_id.is_none() {
            Err(PipelineError::StepFailure(StepFailure {
                step: "Authenticate",
                message: "request has no user".to_string(),
            }))
        } else {
            next.handle(passable)
        }
    }
}

struct MarkHandled;

impl Pipe<Request> for MarkHandled {
    fn handle(&self, mut passable: Request, next: Next<'_, Request>) -> PipeResult<Request> {
        passable.status = 200;
        next.handle(passable)
    }
}

fn main() -> rustpipe::PipelineResult<()> {
    let request = Request {
        user_id: Some(7),
        path: "/account".to_string(),
        status: 0,
    };

    let response = Pipeline::new()
        .send(request)
        .through(vec![Arc::new(Authenticate), Arc::new(MarkHandled)])
        .then_return()?;

    println!("{} -> {}", response.path, response.status);
    Ok(())
}
