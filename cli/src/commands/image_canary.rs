//! Testnet-only native Gemini image preflight/canary.
//!
//! The canary is intentionally a buyer-side request through the GM gateway,
//! not a provider-key or worker health probe. It first reads the gateway's
//! live `/v1/models` availability snapshot for both image SKUs, and only then
//! sends one small, non-streaming native `generateContent` request for each.
//! The response parser retains usage and settled-cost metadata while never
//! retaining or printing the generated image body.

use anyhow::{bail, Context as _, Result};
use reqwest::{header, Client};
use serde::Serialize;
use serde_json::{json, Value};

use gm_miner_cli::{client::build_http_client, network::Network, types::GEMINI_IMAGE_MODELS};

const MODELS_PATH: &str = "/v1/models";
const CREDITS_PATH: &str = "/v1/credits";
const GENERATE_CONTENT_PATH_PREFIX: &str = "/v1beta/models/";
const GENERATE_CONTENT_ACTION: &str = ":generateContent";

// This text is deliberately fixed and intentionally never appears in output,
// logs, or an error. The canary proves native image forwarding and settlement;
// it is not a user prompt or an image-quality check.
const CANARY_PROMPT: &str = "Create a simple abstract square test image.";

/// Run the paid Gemini image canary for the selected network.
///
/// The only supported network is testnet. `buyer_api_key` is the GM buyer key
/// used by the gateway, not either provider key stored for a miner worker. A
/// gateway URL override is useful for local/mock verification; production
/// callers should leave it unset so the network profile supplies the host.
pub(crate) async fn cmd_image_canary(
    network: Network,
    gateway_url: Option<&str>,
    buyer_api_key: &str,
) -> Result<()> {
    ensure_testnet(network)?;
    if buyer_api_key.trim().is_empty() {
        bail!("a funded GM buyer key is required; pass `--buyer-api-key` or set GM_API_KEY");
    }
    let gateway_url = gateway_url.unwrap_or_else(|| network.default_gateway_url());
    let client = build_http_client()?;
    let reports = run_image_canary_with_client(&client, gateway_url, buyer_api_key).await?;
    for report in reports {
        // This is the complete output surface by design. In particular, do
        // not print the request/response body, prompt, image data, or key.
        println!(
            "{}",
            serde_json::to_string(&report).context("serialize image canary report")?
        );
    }
    Ok(())
}

/// A machine-readable, safe reconciliation line emitted after one SKU probe.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct ImageCanaryReport {
    pub(crate) model: String,
    pub(crate) request_id: String,
    pub(crate) usage: UsageDimensions,
    pub(crate) settled_ndollars: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) balance_before_ndollars: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) balance_after_ndollars: Option<u64>,
    /// `ok` when both balance reads agree with the two settled charges;
    /// `unavailable` when the gateway did not expose a readable balance.
    pub(crate) reconciliation: &'static str,
}

/// The usage dimensions needed to audit an image request without exposing its
/// native response body. Gemini's modality details are normalised into the
/// same text/audio/image split used by GM settlement.
#[expect(
    clippy::struct_field_names,
    reason = "wire-facing usage dimensions intentionally share the token suffix"
)]
#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub(crate) struct UsageDimensions {
    pub(crate) input_tokens: u64,
    pub(crate) output_tokens: u64,
    pub(crate) cache_read_tokens: u64,
    pub(crate) cache_write_5m_tokens: u64,
    pub(crate) cache_write_1h_tokens: u64,
    pub(crate) audio_input_tokens: u64,
    pub(crate) audio_output_tokens: u64,
    pub(crate) image_input_tokens: u64,
    pub(crate) image_output_tokens: u64,
    pub(crate) reasoning_tokens: u64,
    pub(crate) total_tokens: u64,
}

#[derive(Debug)]
struct ImageCanaryObservation {
    model: String,
    request_id: String,
    usage: UsageDimensions,
    settled_ndollars: u64,
}

#[derive(Debug, serde::Deserialize)]
struct ModelListResponse {
    #[serde(default)]
    data: Vec<ModelAvailability>,
}

#[derive(Debug, serde::Deserialize)]
struct ModelAvailability {
    id: String,
    #[serde(default)]
    available: bool,
}

/// The injectable core used by the wiremock tests and by the production
/// command. Offer discovery happens before either balance or generation call,
/// making a missing SKU a strict no-spend gate.
async fn run_image_canary_with_client(
    client: &Client,
    gateway_url: &str,
    buyer_api_key: &str,
) -> Result<Vec<ImageCanaryReport>> {
    let gateway_url = gateway_url.trim_end_matches('/');
    if gateway_url.is_empty() {
        bail!("the Gemini image canary gateway URL cannot be empty");
    }
    let missing = fetch_missing_live_offers(client, gateway_url).await?;
    if !missing.is_empty() {
        bail!(
            "Gemini image canary stopped before spending: no live eligible offer for {}",
            missing.join(", ")
        );
    }

    let balance_before = fetch_balance(client, gateway_url, buyer_api_key).await;
    let mut observations = Vec::with_capacity(GEMINI_IMAGE_MODELS.len());
    let mut failures = Vec::new();
    for model in GEMINI_IMAGE_MODELS {
        match send_image_probe(client, gateway_url, buyer_api_key, model).await {
            Ok(observation) => observations.push(observation),
            Err(error) => failures.push(format!("{model}: {error}")),
        }
    }
    // Read the balance even when a provider request failed: it is still useful
    // to tell an operator whether a failed request consumed anything, and this
    // read is never an image/provider call.
    let balance_after = fetch_balance(client, gateway_url, buyer_api_key).await;

    if !failures.is_empty() {
        bail!(
            "Gemini image canary request failure(s): {}",
            failures.join("; ")
        );
    }
    if observations.len() != GEMINI_IMAGE_MODELS.len() {
        bail!("Gemini image canary did not produce one result per SKU");
    }

    let settled_total = observations
        .iter()
        .map(|observation| observation.settled_ndollars)
        .try_fold(0_u64, u64::checked_add)
        .ok_or_else(|| anyhow::anyhow!("Gemini image canary settled amount overflowed"))?;
    let reconciliation = match (balance_before, balance_after) {
        (Some(before), Some(after)) => {
            let observed_debit = before.checked_sub(after).ok_or_else(|| {
                anyhow::anyhow!(
                    "Gemini image canary reconciliation mismatch: balance increased from {before} to {after} nUSD while settled charges total {settled_total} nUSD"
                )
            })?;
            if observed_debit != settled_total {
                bail!(
                    "Gemini image canary reconciliation mismatch: settled charges total {settled_total} nUSD, balance decreased by {observed_debit} nUSD"
                );
            }
            "ok"
        }
        _ => "unavailable",
    };

    Ok(observations
        .into_iter()
        .map(|observation| ImageCanaryReport {
            model: observation.model,
            request_id: observation.request_id,
            usage: observation.usage,
            settled_ndollars: observation.settled_ndollars,
            balance_before_ndollars: balance_before,
            balance_after_ndollars: balance_after,
            reconciliation,
        })
        .collect())
}

fn ensure_testnet(network: Network) -> Result<()> {
    if network != Network::Testnet {
        bail!(
            "Gemini image canary is testnet-only; pass `--network testnet` and use a funded testnet buyer key"
        );
    }
    Ok(())
}

async fn fetch_missing_live_offers(
    client: &Client,
    gateway_url: &str,
) -> Result<Vec<&'static str>> {
    let url = format!("{gateway_url}{MODELS_PATH}?api_shape=generateContent");
    let response = client
        .get(&url)
        .send()
        .await
        .with_context(|| format!("GET {MODELS_PATH}"))?;
    let status = response.status();
    if !status.is_success() {
        bail!("GET {MODELS_PATH} failed ({status})");
    }
    let body = response
        .bytes()
        .await
        .context("read live Gemini offer response")?;
    let list: ModelListResponse =
        serde_json::from_slice(&body).context("parse live Gemini offer response")?;
    Ok(GEMINI_IMAGE_MODELS
        .into_iter()
        .filter(|model| {
            !list
                .data
                .iter()
                .any(|entry| entry.id == *model && entry.available)
        })
        .collect())
}

async fn fetch_balance(client: &Client, gateway_url: &str, buyer_api_key: &str) -> Option<u64> {
    let url = format!("{gateway_url}{CREDITS_PATH}");
    let response = client
        .get(url)
        .header(header::AUTHORIZATION, format!("Bearer {buyer_api_key}"))
        .send()
        .await
        .ok()?;
    if !response.status().is_success() {
        return None;
    }
    let body = response.bytes().await.ok()?;
    let value: Value = serde_json::from_slice(&body).ok()?;
    let balance = value.get("balance")?;
    value_u64(balance, &["nano_usd", "nanoUsd"])
}

async fn send_image_probe(
    client: &Client,
    gateway_url: &str,
    buyer_api_key: &str,
    model: &str,
) -> Result<ImageCanaryObservation> {
    let url =
        format!("{gateway_url}{GENERATE_CONTENT_PATH_PREFIX}{model}{GENERATE_CONTENT_ACTION}");
    let response = client
        .post(url)
        .header(header::CONTENT_TYPE, "application/json")
        .header("x-goog-api-key", buyer_api_key)
        .json(&image_probe_body())
        .send()
        .await
        .with_context(|| format!("POST native Gemini generateContent for {model}"))?;
    let status = response.status();
    let request_id_header = response
        .headers()
        .get("x-gm-request-id")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned);
    if !status.is_success() {
        // Do not read or include the body. Provider/gateway error payloads are
        // not part of this safe reconciliation surface.
        bail!("native Gemini generateContent for {model} failed ({status})");
    }
    let body = response
        .bytes()
        .await
        .with_context(|| format!("read native Gemini response for {model}"))?;
    let value: Value = serde_json::from_slice(&body)
        .with_context(|| format!("parse native Gemini response for {model}"))?;
    let request_id = request_id_header
        .or_else(|| {
            value
                .get("responseId")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .ok_or_else(|| anyhow::anyhow!("native Gemini response for {model} had no request ID"))?;
    let usage = parse_usage_dimensions(&value).ok_or_else(|| {
        anyhow::anyhow!("native Gemini response for {model} had no usage metadata")
    })?;
    let settled_ndollars = parse_settled_ndollars(&value)
        .ok_or_else(|| anyhow::anyhow!("native Gemini response for {model} had no settled nUSD"))?;
    Ok(ImageCanaryObservation {
        model: model.to_owned(),
        request_id,
        usage,
        settled_ndollars,
    })
}

/// Native Gemini `generateContent` request for the canary. The image model
/// accepts IMAGE-only responses; `tools` is intentionally absent, which makes
/// grounding/search impossible. This body is never logged or printed.
fn image_probe_body() -> Value {
    json!({
        "contents": [{
            "role": "user",
            "parts": [{"text": CANARY_PROMPT}]
        }],
        "generationConfig": {
            "candidateCount": 1,
            "responseModalities": ["IMAGE"],
            "imageConfig": {"imageSize": "1K"}
        }
    })
}

fn parse_usage_dimensions(value: &Value) -> Option<UsageDimensions> {
    let usage = value.get("usageMetadata")?;
    if usage.is_null() {
        return None;
    }
    let prompt_total = value_u64(usage, &["promptTokenCount", "prompt_token_count"]).unwrap_or(0);
    let output_total =
        value_u64(usage, &["candidatesTokenCount", "candidates_token_count"]).unwrap_or(0);
    let cache_read = value_u64(
        usage,
        &["cachedContentTokenCount", "cached_content_token_count"],
    )
    .unwrap_or(0);
    let image_input = value_u64(usage, &["imageInputTokenCount", "image_input_token_count"])
        .unwrap_or_else(|| {
            modality_tokens(
                usage,
                &["promptTokensDetails", "prompt_tokens_details"],
                "IMAGE",
            )
        });
    let image_output = value_u64(
        usage,
        &["imageOutputTokenCount", "image_output_token_count"],
    )
    .unwrap_or_else(|| {
        modality_tokens(
            usage,
            &["candidatesTokensDetails", "candidates_tokens_details"],
            "IMAGE",
        )
    });
    let audio_input = modality_tokens(
        usage,
        &["promptTokensDetails", "prompt_tokens_details"],
        "AUDIO",
    );
    let audio_output = modality_tokens(
        usage,
        &["candidatesTokensDetails", "candidates_tokens_details"],
        "AUDIO",
    );
    let thoughts = value_u64(usage, &["thoughtsTokenCount", "thoughts_token_count"]).unwrap_or(0);
    let total = value_u64(usage, &["totalTokenCount", "total_token_count"])
        .unwrap_or_else(|| prompt_total.saturating_add(output_total));
    if prompt_total == 0
        && output_total == 0
        && cache_read == 0
        && image_input == 0
        && image_output == 0
        && audio_input == 0
        && audio_output == 0
        && thoughts == 0
    {
        return None;
    }
    Some(UsageDimensions {
        input_tokens: prompt_total
            .saturating_sub(cache_read)
            .saturating_sub(image_input)
            .saturating_sub(audio_input),
        output_tokens: output_total
            .saturating_sub(image_output)
            .saturating_sub(audio_output),
        cache_read_tokens: cache_read,
        cache_write_5m_tokens: 0,
        cache_write_1h_tokens: 0,
        audio_input_tokens: audio_input,
        audio_output_tokens: audio_output,
        image_input_tokens: image_input,
        image_output_tokens: image_output,
        reasoning_tokens: thoughts,
        total_tokens: total,
    })
}

fn modality_tokens(usage: &Value, keys: &[&str], modality: &str) -> u64 {
    keys.iter()
        .find_map(|key| usage.get(*key))
        .and_then(Value::as_array)
        .map_or(0, |entries| {
            entries
                .iter()
                .filter(|entry| entry.get("modality").and_then(Value::as_str) == Some(modality))
                .filter_map(|entry| value_u64(entry, &["tokenCount", "token_count"]))
                .fold(0_u64, u64::saturating_add)
        })
}

fn parse_settled_ndollars(value: &Value) -> Option<u64> {
    let usage = value.get("usageMetadata");
    usage
        .and_then(|metadata| {
            value_u64(
                metadata,
                &[
                    "costNanoUsd",
                    "cost_nano_usd",
                    "settledNanoUsd",
                    "settled_ndollars",
                ],
            )
        })
        .or_else(|| {
            value_u64(
                value,
                &[
                    "costNanoUsd",
                    "cost_nano_usd",
                    "settledNanoUsd",
                    "settled_ndollars",
                ],
            )
        })
}

fn value_u64(value: &Value, keys: &[&str]) -> Option<u64> {
    keys.iter().find_map(|key| {
        value.get(*key).and_then(|candidate| {
            candidate
                .as_u64()
                .or_else(|| candidate.as_str().and_then(|raw| raw.parse().ok()))
        })
    })
}

#[cfg(test)]
#[expect(
    clippy::expect_used,
    reason = "wiremock assertions intentionally panic on unexpected fixtures"
)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use super::*;
    use wiremock::{
        matchers::{body_json, header, method, path},
        Mock, MockServer, Request, ResponseTemplate,
    };

    fn client() -> Client {
        build_http_client().expect("http client")
    }

    async fn mount_live_offers(server: &MockServer, available: bool) {
        Mock::given(method("GET"))
            .and(path(MODELS_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": GEMINI_IMAGE_MODELS
                    .iter()
                    .map(|model| json!({"id": model, "available": available}))
                    .collect::<Vec<_>>(),
            })))
            .mount(server)
            .await;
    }

    fn native_response(model: &str, request_id: &str, settled_ndollars: u64) -> Value {
        json!({
            "responseId": request_id,
            "modelVersion": model,
            "candidates": [{"content": {"parts": [{"inlineData": {"mimeType": "image/png", "data": "GENERATED_IMAGE_MUST_NOT_BE_PRINTED"}}]}}],
            "usageMetadata": {
                "promptTokenCount": 100,
                "candidatesTokenCount": 1_680,
                "promptTokensDetails": [
                    {"modality": "TEXT", "tokenCount": 40},
                    {"modality": "IMAGE", "tokenCount": 60}
                ],
                "candidatesTokensDetails": [
                    {"modality": "IMAGE", "tokenCount": 1_600},
                    {"modality": "TEXT", "tokenCount": 80}
                ],
                "thoughtsTokenCount": 20,
                "totalTokenCount": 1_780,
                "costNanoUsd": settled_ndollars
            }
        })
    }

    async fn mount_native_responses(server: &MockServer, settled_ndollars: u64) {
        for (index, model) in GEMINI_IMAGE_MODELS.into_iter().enumerate() {
            let response = native_response(model, &format!("gm-request-{index}"), settled_ndollars);
            Mock::given(method("POST"))
                .and(path(format!(
                    "{GENERATE_CONTENT_PATH_PREFIX}{model}{GENERATE_CONTENT_ACTION}"
                )))
                .and(header("x-goog-api-key", "buyer-secret"))
                .and(body_json(image_probe_body()))
                .respond_with(
                    ResponseTemplate::new(200)
                        .insert_header("x-gm-request-id", format!("gm-request-{index}"))
                        .set_body_json(response),
                )
                .mount(server)
                .await;
        }
    }

    async fn mount_credit_sequence(server: &MockServer, before: u64, after: u64) {
        let calls = Arc::new(AtomicUsize::new(0));
        Mock::given(method("GET"))
            .and(path(CREDITS_PATH))
            .respond_with({
                let calls = Arc::clone(&calls);
                move |_request: &Request| {
                    let value = if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                        before
                    } else {
                        after
                    };
                    ResponseTemplate::new(200).set_body_json(json!({
                        "balance": {"nano_usd": value}
                    }))
                }
            })
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn missing_live_offer_fails_before_any_paid_request() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(MODELS_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": [{"id": GEMINI_IMAGE_MODELS[0], "available": true}],
            })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let error = run_image_canary_with_client(&client(), &server.uri(), "buyer-secret")
            .await
            .expect_err("the missing SKU must gate spend");
        let message = error.to_string();
        assert!(message.contains(GEMINI_IMAGE_MODELS[1]), "{message}");
        assert!(message.contains("before spending"), "{message}");
        let requests = server.received_requests().await.expect("requests");
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.method.as_str() == "POST")
                .count(),
            0
        );
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.url.path() == CREDITS_PATH)
                .count(),
            0,
            "the offer gate must run before balance reads too"
        );
    }

    #[tokio::test]
    async fn both_skus_send_native_guarded_requests_and_reconcile_balances() {
        let server = MockServer::start().await;
        mount_live_offers(&server, true).await;
        mount_native_responses(&server, 123).await;
        mount_credit_sequence(&server, 10_000, 9_754).await;

        let reports = run_image_canary_with_client(&client(), &server.uri(), "buyer-secret")
            .await
            .expect("both image requests reconcile");
        assert_eq!(reports.len(), 2);
        assert!(reports.iter().all(|report| report.reconciliation == "ok"));
        assert!(reports
            .iter()
            .all(|report| report.balance_before_ndollars == Some(10_000)));
        assert!(reports
            .iter()
            .all(|report| report.balance_after_ndollars == Some(9_754)));
        assert_eq!(reports[0].usage.image_input_tokens, 60);
        assert_eq!(reports[0].usage.image_output_tokens, 1_600);
        assert_eq!(reports[0].settled_ndollars, 123);

        let requests = server.received_requests().await.expect("requests");
        let native_requests: Vec<_> = requests
            .iter()
            .filter(|request| request.method.as_str() == "POST")
            .collect();
        assert_eq!(native_requests.len(), 2);
        for request in native_requests {
            let body: Value = serde_json::from_slice(&request.body).expect("native body");
            assert_eq!(body["generationConfig"]["candidateCount"], 1);
            assert_eq!(body["generationConfig"]["imageConfig"]["imageSize"], "1K");
            assert_eq!(
                body["generationConfig"]["responseModalities"],
                json!(["IMAGE"])
            );
            assert!(body.get("tools").is_none(), "grounding must be absent");
            assert_eq!(
                request
                    .headers
                    .get("x-goog-api-key")
                    .and_then(|value| value.to_str().ok()),
                Some("buyer-secret")
            );
        }
    }

    #[tokio::test]
    async fn balance_debit_mismatch_fails_reconciliation() {
        let server = MockServer::start().await;
        mount_live_offers(&server, true).await;
        mount_native_responses(&server, 123).await;
        mount_credit_sequence(&server, 10_000, 9_999).await;

        let error = run_image_canary_with_client(&client(), &server.uri(), "buyer-secret")
            .await
            .expect_err("a balance mismatch must fail the canary");
        assert!(error.to_string().contains("reconciliation mismatch"));
    }

    #[test]
    fn native_response_parser_keeps_only_safe_reconciliation_fields() {
        let value = native_response("gemini-3.1-flash-image", "request-1", 321);
        let usage = parse_usage_dimensions(&value).expect("usage");
        assert_eq!(usage.input_tokens, 40);
        assert_eq!(usage.output_tokens, 80);
        assert_eq!(usage.image_input_tokens, 60);
        assert_eq!(usage.image_output_tokens, 1_600);
        assert_eq!(usage.reasoning_tokens, 20);
        assert_eq!(parse_settled_ndollars(&value), Some(321));
        let report = ImageCanaryReport {
            model: "gemini-3.1-flash-image".to_owned(),
            request_id: "request-1".to_owned(),
            usage,
            settled_ndollars: 321,
            balance_before_ndollars: Some(1_000),
            balance_after_ndollars: Some(679),
            reconciliation: "ok",
        };
        let output = serde_json::to_string(&report).expect("safe report");
        assert!(output.contains("request-1"));
        assert!(output.contains("image_input_tokens"));
        assert!(output.contains("321"));
        assert!(!output.contains("GENERATED_IMAGE_MUST_NOT_BE_PRINTED"));
        assert!(!output.contains(CANARY_PROMPT));
        assert!(!output.contains("buyer-secret"));
    }

    #[test]
    fn testnet_gate_refuses_mainnet_before_building_a_client() {
        let error = ensure_testnet(Network::Mainnet).expect_err("mainnet is forbidden");
        assert!(error.to_string().contains("testnet-only"));
    }
}
