#[derive(Debug, failure::Fail)]
pub enum NodeError {
    #[fail(display = "Invalid argument: {}", 0)]
    InvalidArg(String),
    #[fail(display = "Invalid data: {}", 0)]
    InvalidData(String),
    #[fail(display = "Invalid operation: {}", 0)]
    InvalidOperation(String),
}
