use std::sync::Arc;

use rustpipe::{Next, Pipe, PipeResult, Pipeline};

#[derive(Debug)]
struct AxumLikeRequest {
    method: &'static str,
    path: &'static str,
    headers: Vec<(&'static str, &'static str)>,
}

#[derive(Debug)]
struct AxumLikeResponse {
    status: u16,
    body: String,
    headers: Vec<(&'static str, &'static str)>,
}

struct RequestLogger;

impl Pipe<AxumLikeRequest> for RequestLogger {
    fn handle(
        &self,
        passable: AxumLikeRequest,
        next: Next<'_, AxumLikeRequest>,
    ) -> PipeResult<AxumLikeRequest> {
        println!("{} {}", passable.method, passable.path);
        next.handle(passable)
    }
}

struct AddRequestId;

impl Pipe<AxumLikeRequest> for AddRequestId {
    fn handle(
        &self,
        mut passable: AxumLikeRequest,
        next: Next<'_, AxumLikeRequest>,
    ) -> PipeResult<AxumLikeRequest> {
        passable.headers.push(("x-request-id", "req-123"));
        next.handle(passable)
    }
}

fn handler(request: AxumLikeRequest) -> AxumLikeResponse {
    AxumLikeResponse {
        status: 200,
        body: format!("handled {}", request.path),
        headers: request.headers,
    }
}

fn main() -> rustpipe::PipelineResult<()> {
    let request = AxumLikeRequest {
        method: "GET",
        path: "/users",
        headers: Vec::new(),
    };

    let response = Pipeline::new()
        .send(request)
        .through(vec![Arc::new(RequestLogger), Arc::new(AddRequestId)])
        .then(handler)?;

    println!(
        "{} {} {:?}",
        response.status, response.body, response.headers
    );
    Ok(())
}
