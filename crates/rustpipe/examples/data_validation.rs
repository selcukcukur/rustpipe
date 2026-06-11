use std::sync::Arc;

use rustpipe::{PipelineError, StepFailure, TransformPipe, TransformPipeResult, TransformPipeline};

#[derive(Debug)]
struct Signup {
    email: String,
    password: String,
    normalized: bool,
}

struct NormalizeEmail;

impl TransformPipe<Signup> for NormalizeEmail {
    fn handle(&self, mut passable: Signup) -> TransformPipeResult<Signup> {
        passable.email = passable.email.trim().to_lowercase();
        passable.normalized = true;
        Ok(passable)
    }
}

struct ValidateEmail;

impl TransformPipe<Signup> for ValidateEmail {
    fn handle(&self, passable: Signup) -> TransformPipeResult<Signup> {
        if passable.email.contains('@') {
            Ok(passable)
        } else {
            Err(PipelineError::StepFailure(StepFailure {
                step: "ValidateEmail",
                message: "email must contain @".to_string(),
            }))
        }
    }
}

struct ValidatePassword;

impl TransformPipe<Signup> for ValidatePassword {
    fn handle(&self, passable: Signup) -> TransformPipeResult<Signup> {
        if passable.password.len() >= 12 {
            Ok(passable)
        } else {
            Err(PipelineError::StepFailure(StepFailure {
                step: "ValidatePassword",
                message: "password must be at least 12 characters".to_string(),
            }))
        }
    }
}

fn main() -> rustpipe::PipelineResult<()> {
    let signup = Signup {
        email: " USER@example.COM ".to_string(),
        password: "correct horse".to_string(),
        normalized: false,
    };

    let signup = TransformPipeline::new()
        .send(signup)
        .through(vec![
            Arc::new(NormalizeEmail),
            Arc::new(ValidateEmail),
            Arc::new(ValidatePassword),
        ])
        .then_return()?;

    println!("{signup:?}");
    Ok(())
}
