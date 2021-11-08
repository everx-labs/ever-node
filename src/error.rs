#[derive(Debug, failure::Fail)]
pub enum NodeError {
    #[fail(display = "Invalid argument: {}", 0)]
    InvalidArg(String),
    #[fail(display = "Invalid data: {}", 0)]
    InvalidData(String),
    #[fail(display = "Invalid operation: {}", 0)]
    InvalidOperation(String),
    #[fail(display = "{}", 0)]
    ValidatorReject(String),
    #[fail(display = "{}", 0)]
    ValidatorSoftReject(String),
    #[cfg(feature = "external_db")]
    #[fail(display = "{}", 0)]
    #[allow(dead_code)]
    Other(String),
}
