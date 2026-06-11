use std::sync::{Arc, Mutex};

use rustpipe::{Next, Pipe, PipeResult, Pipeline, PipelineError, StepFailure};

#[derive(Clone, Debug, PartialEq, Eq)]
struct RequestContext {
    user_id: Option<u64>,
    path: String,
    headers: Vec<(String, String)>,
    events: Vec<String>,
    status: u16,
}

impl RequestContext {
    fn new(path: &str) -> Self {
        Self {
            user_id: Some(42),
            path: path.to_string(),
            headers: Vec::new(),
            events: Vec::new(),
            status: 200,
        }
    }
}

struct Trace(&'static str);

impl Pipe<RequestContext> for Trace {
    fn handle(
        &self,
        mut passable: RequestContext,
        next: Next<'_, RequestContext>,
    ) -> PipeResult<RequestContext> {
        passable.events.push(format!("{}:before", self.0));

        let mut response = next.handle(passable)?;
        response.events.push(format!("{}:after", self.0));

        Ok(response)
    }
}

struct RequireUser;

impl Pipe<RequestContext> for RequireUser {
    fn handle(
        &self,
        passable: RequestContext,
        next: Next<'_, RequestContext>,
    ) -> PipeResult<RequestContext> {
        if passable.user_id.is_none() {
            Err(PipelineError::StepFailure(StepFailure {
                step: "RequireUser",
                message: "missing authenticated user".to_string(),
            }))
        } else {
            next.handle(passable)
        }
    }
}

struct AddHeader(&'static str, &'static str);

impl Pipe<RequestContext> for AddHeader {
    fn handle(
        &self,
        passable: RequestContext,
        next: Next<'_, RequestContext>,
    ) -> PipeResult<RequestContext> {
        let mut response = next.handle(passable)?;
        response
            .headers
            .push((self.0.to_string(), self.1.to_string()));
        Ok(response)
    }
}

struct MaintenanceMode;

impl Pipe<RequestContext> for MaintenanceMode {
    fn handle(
        &self,
        mut passable: RequestContext,
        _next: Next<'_, RequestContext>,
    ) -> PipeResult<RequestContext> {
        passable.status = 503;
        passable
            .events
            .push("maintenance:short-circuit".to_string());
        Ok(passable)
    }
}

#[test]
fn middleware_wraps_downstream_response_in_lifo_order() {
    let result = Pipeline::new()
        .send(RequestContext::new("/dashboard"))
        .through(vec![
            Arc::new(Trace("outer")),
            Arc::new(Trace("inner")),
            Arc::new(AddHeader("x-powered-by", "rustpipe")),
        ])
        .then_return()
        .unwrap();

    assert_eq!(
        result.events,
        vec!["outer:before", "inner:before", "inner:after", "outer:after"]
    );
    assert_eq!(
        result.headers,
        vec![("x-powered-by".to_string(), "rustpipe".to_string())]
    );
}

#[test]
fn middleware_then_maps_final_context() {
    let status = Pipeline::new()
        .send(RequestContext::new("/health"))
        .through(vec![Arc::new(Trace("trace"))])
        .then(|context| context.status)
        .unwrap();

    assert_eq!(status, 200);
}

#[test]
fn middleware_short_circuit_skips_later_pipes() {
    let result = Pipeline::new()
        .send(RequestContext::new("/deploy"))
        .through(vec![
            Arc::new(Trace("outer")),
            Arc::new(MaintenanceMode),
            Arc::new(Trace("never")),
        ])
        .then_return()
        .unwrap();

    assert_eq!(result.status, 503);
    assert_eq!(
        result.events,
        vec!["outer:before", "maintenance:short-circuit", "outer:after"]
    );
}

#[test]
fn middleware_errors_stop_the_chain_and_run_finally() {
    let called = Arc::new(Mutex::new(false));
    let called_in_finally = Arc::clone(&called);
    let mut request = RequestContext::new("/admin");
    request.user_id = None;

    let result = Pipeline::new()
        .send(request)
        .through(vec![Arc::new(RequireUser), Arc::new(Trace("never"))])
        .finally(move |result| {
            *called_in_finally.lock().unwrap() = result.is_err();
        })
        .then_return();

    assert!(matches!(result, Err(PipelineError::StepFailure(_))));
    assert!(*called.lock().unwrap());
}

#[test]
fn middleware_rescue_returns_fallback_context() {
    let mut request = RequestContext::new("/admin");
    request.user_id = None;

    let result = Pipeline::new()
        .send(request)
        .through(vec![Arc::new(RequireUser)])
        .rescue(|_| {
            let mut fallback = RequestContext::new("/login");
            fallback.status = 401;
            fallback
        })
        .unwrap();

    assert_eq!(result.status, 401);
    assert_eq!(result.path, "/login");
}
