//! Role-based bearer-token authentication.

use std::path::Path;
use std::str::FromStr;

use axum::extract::{Extension, State};
use axum::http::{header, Request};
use axum::middleware::Next;
use axum::response::Response;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::ServerError;
use crate::state::AppState;

/// HTTP authorization roles, ordered from least to most privileged.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Observer,
    Controller,
    Administrator,
}

impl FromStr for Role {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "observer" => Ok(Self::Observer),
            "controller" => Ok(Self::Controller),
            "administrator" => Ok(Self::Administrator),
            _ => Err(format!(
                "invalid role `{value}` (expected observer|controller|administrator)"
            )),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct AuthFile {
    #[serde(default)]
    unauthenticated_role: Option<Role>,
    tokens: Vec<TokenConfig>,
}

#[derive(Debug, Clone, Deserialize)]
struct TokenConfig {
    name: String,
    sha256: String,
    role: Role,
}

#[derive(Debug, Clone)]
struct TokenRecord {
    name: String,
    sha256: [u8; 32],
    role: Role,
}

/// Effective authentication policy loaded once at startup.
#[derive(Debug, Clone)]
pub struct AuthPolicy {
    tokens: Vec<TokenRecord>,
    unauthenticated_role: Option<Role>,
}

impl AuthPolicy {
    pub fn from_file(path: &Path) -> anyhow::Result<Self> {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("failed to read auth config {path:?}: {e}"))?;
        let parsed: AuthFile = toml::from_str(&raw)
            .map_err(|e| anyhow::anyhow!("invalid auth config {path:?}: {e}"))?;
        if parsed.tokens.is_empty() {
            anyhow::bail!("auth config must contain at least one [[tokens]] entry");
        }
        let unauthenticated_role = parsed.unauthenticated_role;
        let mut tokens = Vec::with_capacity(parsed.tokens.len());
        for entry in parsed.tokens {
            let bytes = hex_decode_sha256(&entry.sha256)
                .map_err(|e| anyhow::anyhow!("invalid sha256 for token {:?}: {e}", entry.name))?;
            if tokens.iter().any(|t: &TokenRecord| t.name == entry.name) {
                anyhow::bail!("duplicate token name {:?}", entry.name);
            }
            tokens.push(TokenRecord {
                name: entry.name,
                sha256: bytes,
                role: entry.role,
            });
        }
        Ok(Self {
            tokens,
            unauthenticated_role,
        })
    }

    pub fn unauthenticated(role: Role) -> Self {
        Self {
            tokens: Vec::new(),
            unauthenticated_role: Some(role),
        }
    }

    fn authenticate(&self, token: Option<&str>) -> Option<Principal> {
        if let Some(token) = token {
            let digest: [u8; 32] = Sha256::digest(token.as_bytes()).into();
            return self.tokens.iter().find_map(|record| {
                constant_time_eq(&digest, &record.sha256).then(|| Principal {
                    name: record.name.clone(),
                    role: record.role,
                })
            });
        }
        self.unauthenticated_role.map(|role| Principal {
            name: "unauthenticated".to_string(),
            role,
        })
    }

    pub fn unauthenticated_role(&self) -> Option<Role> {
        self.unauthenticated_role
    }
}

#[derive(Debug, Clone)]
pub struct Principal {
    pub name: String,
    pub role: Role,
}

pub async fn require_bearer(
    State(state): State<AppState>,
    mut req: Request<axum::body::Body>,
    next: Next,
) -> Result<Response, ServerError> {
    let provided = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::trim);
    let principal = state
        .auth
        .authenticate(provided)
        .ok_or_else(|| ServerError::unauthorized("missing or invalid bearer token"))?;
    req.extensions_mut().insert(principal);
    Ok(next.run(req).await)
}

/// Extract a principal and enforce a minimum role in handlers.
pub fn require_role(
    Extension(principal): Extension<Principal>,
    required: Role,
) -> Result<Principal, ServerError> {
    if principal.role < required {
        return Err(ServerError::forbidden(format!(
            "{} role required (token {:?} has {:?})",
            match required {
                Role::Observer => "observer",
                Role::Controller => "controller",
                Role::Administrator => "administrator",
            },
            principal.name,
            principal.role
        )));
    }
    Ok(principal)
}

fn constant_time_eq(a: &[u8; 32], b: &[u8; 32]) -> bool {
    let mut diff = 0u8;
    for (left, right) in a.iter().zip(b.iter()) {
        diff |= left ^ right;
    }
    diff == 0
}

fn hex_decode_sha256(value: &str) -> Result<[u8; 32], &'static str> {
    let value = value.trim();
    if value.len() != 64 {
        return Err("must contain exactly 64 hexadecimal characters");
    }
    let mut out = [0u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let text = std::str::from_utf8(pair).map_err(|_| "must be ASCII hexadecimal")?;
        out[index] = u8::from_str_radix(text, 16).map_err(|_| "must be hexadecimal")?;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hashes_are_compared_and_roles_preserved() {
        let hash: [u8; 32] = Sha256::digest(b"secret").into();
        let policy = AuthPolicy {
            tokens: vec![TokenRecord {
                name: "monitor".into(),
                sha256: hash,
                role: Role::Observer,
            }],
            unauthenticated_role: None,
        };
        let principal = policy.authenticate(Some("secret")).unwrap();
        assert_eq!(principal.name, "monitor");
        assert_eq!(principal.role, Role::Observer);
        assert!(policy.authenticate(Some("wrong")).is_none());
    }

    #[test]
    fn token_acl_can_have_a_lower_unauthenticated_fallback() {
        let hash: [u8; 32] = Sha256::digest(b"admin-secret").into();
        let policy = AuthPolicy {
            tokens: vec![TokenRecord {
                name: "owner".into(),
                sha256: hash,
                role: Role::Administrator,
            }],
            unauthenticated_role: Some(Role::Controller),
        };

        let administrator = policy.authenticate(Some("admin-secret")).unwrap();
        assert_eq!(administrator.name, "owner");
        assert_eq!(administrator.role, Role::Administrator);

        let unauthenticated = policy.authenticate(None).unwrap();
        assert_eq!(unauthenticated.name, "unauthenticated");
        assert_eq!(unauthenticated.role, Role::Controller);

        // A caller that explicitly supplied a bad credential is rejected; it
        // is not silently treated as an unauthenticated request.
        assert!(policy.authenticate(Some("wrong")).is_none());
    }

    #[test]
    fn auth_file_parses_unauthenticated_fallback_with_tokens() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("auth.toml");
        let hash = crate::state::sha256_hex(b"admin-secret");
        std::fs::write(
            &path,
            format!(
                "unauthenticated_role = \"controller\"\n\n\
                 [[tokens]]\n\
                 name = \"owner\"\n\
                 sha256 = \"{hash}\"\n\
                 role = \"administrator\"\n"
            ),
        )
        .unwrap();

        let policy = AuthPolicy::from_file(&path).unwrap();
        assert_eq!(policy.authenticate(None).unwrap().role, Role::Controller);
        assert_eq!(
            policy.authenticate(Some("admin-secret")).unwrap().role,
            Role::Administrator
        );
    }
}
