use std::sync::Arc;

use rustpipe::{
    Next, Pipe, PipeResult, Pipeline, TransformPipe, TransformPipeResult, TransformPipeline,
};

#[derive(Clone, Debug, PartialEq, Eq)]
struct Event {
    id: usize,
    tenant: String,
    payload: String,
    priority: u8,
    accepted: bool,
    audit: Vec<String>,
}

struct AttachTenant(&'static str);

impl Pipe<Vec<Event>> for AttachTenant {
    fn handle(
        &self,
        mut passable: Vec<Event>,
        next: Next<'_, Vec<Event>>,
    ) -> PipeResult<Vec<Event>> {
        for event in &mut passable {
            event.tenant = self.0.to_string();
            event.audit.push("tenant".to_string());
        }

        next.handle(passable)
    }
}

struct RejectEmptyBatch;

impl Pipe<Vec<Event>> for RejectEmptyBatch {
    fn handle(&self, passable: Vec<Event>, next: Next<'_, Vec<Event>>) -> PipeResult<Vec<Event>> {
        if passable.is_empty() {
            Ok(passable)
        } else {
            next.handle(passable)
        }
    }
}

struct MarkAccepted;

impl Pipe<Vec<Event>> for MarkAccepted {
    fn handle(
        &self,
        mut passable: Vec<Event>,
        next: Next<'_, Vec<Event>>,
    ) -> PipeResult<Vec<Event>> {
        for event in &mut passable {
            event.accepted = true;
            event.audit.push("accepted".to_string());
        }

        next.handle(passable)
    }
}

struct NormalizePayload;

impl TransformPipe<Vec<Event>> for NormalizePayload {
    fn handle(&self, mut passable: Vec<Event>) -> TransformPipeResult<Vec<Event>> {
        for event in &mut passable {
            event.payload = event.payload.trim().to_ascii_uppercase();
            event.audit.push("normalized".to_string());
        }

        Ok(passable)
    }
}

struct BoostPriority;

impl TransformPipe<Vec<Event>> for BoostPriority {
    fn handle(&self, mut passable: Vec<Event>) -> TransformPipeResult<Vec<Event>> {
        for event in &mut passable {
            if event.id % 10 == 0 {
                event.priority = event.priority.saturating_add(5);
                event.audit.push("boosted".to_string());
            }
        }

        Ok(passable)
    }
}

fn events(count: usize) -> Vec<Event> {
    (0..count)
        .map(|id| Event {
            id,
            tenant: String::new(),
            payload: format!(" event-{id} "),
            priority: (id % 5) as u8,
            accepted: false,
            audit: Vec::new(),
        })
        .collect()
}

#[test]
fn full_pipeline_stress_processes_one_thousand_items() {
    let middleware_output = Pipeline::new()
        .send(events(1_000))
        .through(vec![
            Arc::new(RejectEmptyBatch),
            Arc::new(AttachTenant("acme")),
            Arc::new(MarkAccepted),
        ])
        .then_return()
        .unwrap();

    let output = TransformPipeline::new()
        .send(middleware_output)
        .through(vec![Arc::new(NormalizePayload), Arc::new(BoostPriority)])
        .then_return()
        .unwrap();

    assert_eq!(output.len(), 1_000);
    assert!(output.iter().all(|event| event.tenant == "acme"));
    assert!(output.iter().all(|event| event.accepted));
    assert_eq!(output[0].payload, "EVENT-0");
    assert_eq!(output[0].priority, 5);
    assert_eq!(
        output[0].audit,
        vec!["tenant", "accepted", "normalized", "boosted"]
    );
    assert_eq!(output[999].payload, "EVENT-999");
    assert_eq!(output[999].audit, vec!["tenant", "accepted", "normalized"]);
}

#[test]
fn empty_stress_batch_short_circuits_cleanly() {
    let output = Pipeline::new()
        .send(Vec::<Event>::new())
        .through(vec![
            Arc::new(RejectEmptyBatch),
            Arc::new(AttachTenant("never")),
        ])
        .then_return()
        .unwrap();

    assert!(output.is_empty());
}
