pub type Result<T> = std::result::Result<T, QuackError>;

#[derive(Debug, thiserror::Error)]
pub enum QuackError {
    #[error("quack protocol error: {0}")]
    Protocol(String),
    #[error("quack server error: {0}")]
    Server(String),
    #[error("unsupported quack type: {0}")]
    UnsupportedType(String),
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("url parse error: {0}")]
    Url(#[from] url::ParseError),
    #[error("utf-8 error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
}

impl QuackError {
    pub fn protocol(message: impl Into<String>) -> Self {
        Self::Protocol(message.into())
    }

    pub fn server(message: impl Into<String>) -> Self {
        Self::Server(message.into())
    }

    pub fn unsupported(message: impl Into<String>) -> Self {
        Self::UnsupportedType(message.into())
    }
}
