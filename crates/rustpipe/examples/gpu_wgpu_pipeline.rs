use std::sync::Arc;

use rustpipe::{TransformPipe, TransformPipeResult, TransformPipeline};

#[derive(Debug)]
struct RenderFrame {
    label: String,
    command_log: Vec<String>,
    vertex_count: u32,
}

struct UploadBuffers;

impl TransformPipe<RenderFrame> for UploadBuffers {
    fn handle(&self, mut passable: RenderFrame) -> TransformPipeResult<RenderFrame> {
        passable.command_log.push("wgpu:upload-buffers".to_string());
        Ok(passable)
    }
}

struct EncodeRenderPass;

impl TransformPipe<RenderFrame> for EncodeRenderPass {
    fn handle(&self, mut passable: RenderFrame) -> TransformPipeResult<RenderFrame> {
        passable.command_log.push(format!(
            "wgpu:encode-render-pass vertices={}",
            passable.vertex_count
        ));
        Ok(passable)
    }
}

struct SubmitQueue;

impl TransformPipe<RenderFrame> for SubmitQueue {
    fn handle(&self, mut passable: RenderFrame) -> TransformPipeResult<RenderFrame> {
        passable.command_log.push("wgpu:queue-submit".to_string());
        Ok(passable)
    }
}

fn main() -> rustpipe::PipelineResult<()> {
    let frame = RenderFrame {
        label: "main-pass".to_string(),
        command_log: Vec::new(),
        vertex_count: 36,
    };

    let frame = TransformPipeline::new()
        .send(frame)
        .through(vec![
            Arc::new(UploadBuffers),
            Arc::new(EncodeRenderPass),
            Arc::new(SubmitQueue),
        ])
        .then_return()?;

    println!("{} {:?}", frame.label, frame.command_log);
    Ok(())
}
