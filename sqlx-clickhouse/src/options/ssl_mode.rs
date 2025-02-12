use crate::error::Error;
use std::str::FromStr;

/// Options for controlling the level of protection provided for ClickHouse SSL connections.
///
/// It is used by the [`ssl_mode`](super::ClickHouseConnectOptions::ssl_mode) method.
#[derive(Debug, Clone, Copy, Default)]
pub enum ClickHouseSslMode {
    /// Only try a non-SSL connection.
    Disable,

    /// First try a non-SSL connection; if that fails, try an SSL connection.
    Allow,

    /// First try an SSL connection; if that fails, try a non-SSL connection.
    ///
    /// This is the default if no other mode is specified.
    #[default]
    Prefer,

    /// Only try an SSL connection. If a root CA file is present, verify the connection
    /// in the same way as if `VerifyCa` was specified.
    Require,

    /// Only try an SSL connection, and verify that the server certificate is issued by a
    /// trusted certificate authority (CA).
    VerifyCa,

    /// Only try an SSL connection; verify that the server certificate is issued by a trusted
    /// CA and that the requested server host name matches that in the certificate.
    VerifyFull,
}

impl FromStr for ClickHouseSslMode {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Error> {
        Ok(match &*s.to_ascii_lowercase() {
            "disable" => ClickHouseSslMode::Disable,
            "allow" => ClickHouseSslMode::Allow,
            "prefer" => ClickHouseSslMode::Prefer,
            "require" => ClickHouseSslMode::Require,
            "verify-ca" => ClickHouseSslMode::VerifyCa,
            "verify-full" => ClickHouseSslMode::VerifyFull,

            _ => {
                return Err(Error::Configuration(
                    format!("unknown value {s:?} for `ssl_mode`").into(),
                ));
            }
        })
    }
}
