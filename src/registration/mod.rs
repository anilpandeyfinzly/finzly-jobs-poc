//! Job-registration feature: parse the finzly job-config YAML and persist it.

pub mod handler;
pub mod model;
pub mod repository;

use axum::routing::post;
use axum::Router;

/// Routes owned by the registration feature.
pub fn router() -> Router {
    Router::new().route("/job/registrations", post(handler::register_job))
}
