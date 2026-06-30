//! HTTP layer for job registration.

use axum::extract::Multipart;
use axum::http::StatusCode;
use axum::response::IntoResponse;

use super::model::Config;
use super::repository;

/// Parse a job-config YAML document into the registration [`Config`].
pub fn read_yaml_config(job_yaml: &str) -> serde_yaml::Result<Config> {
    serde_yaml::from_str::<Config>(job_yaml)
}

/// POST /job/registrations — accept a multipart YAML upload and persist the jobs.
pub async fn register_job(
    mut multipart: Multipart,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let field = multipart
        .next_field()
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?
        .ok_or((StatusCode::BAD_REQUEST, "No file uploaded".to_string()))?;
    if let Some(file_name) = field.file_name() {
        println!("Uploaded file: {file_name}");
        if !(file_name.ends_with(".yml") || file_name.ends_with(".yaml")) {
            return Err((
                StatusCode::BAD_REQUEST,
                "Only .yml or .yaml files are supported".to_string(),
            ));
        }
    }

    let yaml = field
        .text()
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;

    // Parse the YAML.
    let config = read_yaml_config(&yaml)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid YAML: {e}")))?;

    // Save registration to PostgreSQL (validation / event publishing still TODO).
    let count = repository::save_registration(&config)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("DB insert failed: {e}")))?;

    println!(
        "Registered {count} job(s) for service {}",
        config.finzly.job.service
    );

    Ok((StatusCode::CREATED, format!("Registered {count} job(s)")))
}

#[cfg(test)]
mod tests {
    use tokio::fs::read_to_string;

    use super::*;

    #[tokio::test]
    async fn yml_test() {
        let read_data = read_to_string("../job-config.yml").await.unwrap();
        let read_yml = read_yaml_config(read_data.as_str());

        assert!(read_yml.is_ok());
        let yml_data = read_yml.unwrap();

        println!("test:: {:?}", yml_data);
        assert_eq!(yml_data.finzly.job.service, "settlement-service");
    }
}
