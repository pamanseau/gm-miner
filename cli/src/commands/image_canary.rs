//! Testnet-only native Gemini image preflight/canary.
//!
//! The canary is intentionally a buyer-side request through the GM gateway,
//! not a provider-key or worker health probe. It first reads the gateway's
//! live `/v1/models` availability snapshot for both image SKUs, and only then
//! sends one small, non-streaming native `generateContent` request for each.
//! The response parser retains usage and settled-cost metadata while never
//! retaining or printing the generated image body.

use anyhow::{bail, Context as _, Result};
use reqwest::{header, Client, Response};
use serde::Serialize;
use serde_json::{json, Value};
use std::fmt;

use gm_miner_cli::{client::build_http_client, network::Network, types::GEMINI_IMAGE_MODELS};

const MODELS_PATH: &str = "/v1/models";
const CREDITS_PATH: &str = "/v1/credits";
const GENERATE_CONTENT_PATH_PREFIX: &str = "/v1beta/models/";
const GENERATE_CONTENT_ACTION: &str = ":generateContent";

// The gateway accepts request bodies up to 16 MiB. Keep the native image
// response reader bounded by that same conservative transport limit so a
// malformed or unexpectedly large response cannot grow an unbounded Vec. A
// normal 1K image response is well below this ceiling.
const MAX_NATIVE_IMAGE_RESPONSE_BYTES: usize = 16 * 1024 * 1024;

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
    match run_image_canary_with_client(&client, gateway_url, buyer_api_key).await {
        Ok(reports) => print_reports(&reports),
        Err(error) => {
            // A generation request can fail after an earlier SKU has already
            // been charged. Preserve and print every safe success report before
            // returning a non-zero result for the partial run.
            if let Some(partial) = error.downcast_ref::<PartialCanaryFailure>() {
                print_reports(&partial.reports)?;
                return Err(anyhow::anyhow!(partial.message.clone()));
            }
            Err(error)
        }
    }
}

fn print_reports(reports: &[ImageCanaryReport]) -> Result<()> {
    for report in reports {
        // This is the complete output surface by design. In particular, do
        // not print the request/response body, prompt, image data, or key.
        println!(
            "{}",
            serde_json::to_string(report).context("serialize image canary report")?
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
    #[serde(skip_serializing_if = "Option::is_none")]
    /// The observed run-level debit, when both balances are available and the
    /// balance moved down. During a partial run it is evidence for the whole
    /// run and must not be attributed to one SKU.
    pub(crate) balance_delta_ndollars: Option<u64>,
    /// `ok` when both balance reads agree with all settled charges;
    /// `unavailable` when the gateway did not expose a readable balance;
    /// `partial` when at least one SKU failed after another produced evidence;
    /// `mismatch` when the observed balance does not reconcile.
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
    /// Gemini's tool-use prompt tokens are separate from prompt and candidate
    /// tokens. The canary does not send tools, but retaining this field keeps
    /// the usage evidence faithful when a gateway fixture includes it.
    pub(crate) tool_use_prompt_tokens: u64,
    pub(crate) total_tokens: u64,
}

#[derive(Debug)]
struct ImageCanaryObservation {
    model: String,
    request_id: String,
    usage: UsageDimensions,
    settled_ndollars: u64,
}

#[derive(Debug)]
struct PartialCanaryFailure {
    reports: Vec<ImageCanaryReport>,
    message: String,
}

impl fmt::Display for PartialCanaryFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for PartialCanaryFailure {}

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
#[expect(
    clippy::too_many_lines,
    reason = "The canary keeps offer gating, both probes, and reconciliation in one transactional flow"
)]
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
        let reports =
            reports_for_observations(observations, balance_before, balance_after, "partial");
        return Err(PartialCanaryFailure {
            reports,
            message: format!(
                "Gemini image canary partial failure: request failure(s): {}",
                failures.join("; ")
            ),
        }
        .into());
    }
    if observations.len() != GEMINI_IMAGE_MODELS.len() {
        return Err(PartialCanaryFailure {
            reports: reports_for_observations(
                observations,
                balance_before,
                balance_after,
                "partial",
            ),
            message: "Gemini image canary partial failure: did not produce one result per SKU"
                .to_owned(),
        }
        .into());
    }

    let Some(settled_total) = observations
        .iter()
        .map(|observation| observation.settled_ndollars)
        .try_fold(0_u64, u64::checked_add)
    else {
        return Err(PartialCanaryFailure {
            reports: reports_for_observations(
                observations,
                balance_before,
                balance_after,
                "mismatch",
            ),
            message: "Gemini image canary reconciliation mismatch: settled amount overflowed"
                .to_owned(),
        }
        .into());
    };
    let reconciliation = match (balance_before, balance_after) {
        (Some(before), Some(after)) => {
            let Some(observed_debit) = before.checked_sub(after) else {
                return Err(PartialCanaryFailure {
                    reports: reports_for_observations(
                        observations,
                        balance_before,
                        balance_after,
                        "mismatch",
                    ),
                    message: format!(
                        "Gemini image canary reconciliation mismatch: balance increased from {before} to {after} nUSD while settled charges total {settled_total} nUSD"
                    ),
                }
                .into());
            };
            if observed_debit != settled_total {
                return Err(PartialCanaryFailure {
                    reports: reports_for_observations(
                        observations,
                        balance_before,
                        balance_after,
                        "mismatch",
                    ),
                    message: format!(
                        "Gemini image canary reconciliation mismatch: settled charges total {settled_total} nUSD, balance decreased by {observed_debit} nUSD"
                    ),
                }
                .into());
            }
            "ok"
        }
        _ => "unavailable",
    };

    Ok(reports_for_observations(
        observations,
        balance_before,
        balance_after,
        reconciliation,
    ))
}

fn reports_for_observations(
    observations: Vec<ImageCanaryObservation>,
    balance_before: Option<u64>,
    balance_after: Option<u64>,
    reconciliation: &'static str,
) -> Vec<ImageCanaryReport> {
    let balance_delta_ndollars =
        balance_before.and_then(|before| balance_after.and_then(|after| before.checked_sub(after)));
    observations
        .into_iter()
        .map(|observation| ImageCanaryReport {
            model: observation.model,
            request_id: observation.request_id,
            usage: observation.usage,
            settled_ndollars: observation.settled_ndollars,
            balance_before_ndollars: balance_before,
            balance_after_ndollars: balance_after,
            balance_delta_ndollars,
            reconciliation,
        })
        .collect()
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
    let body = read_bounded_native_response(response, model).await?;
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

async fn read_bounded_native_response(mut response: Response, model: &str) -> Result<Vec<u8>> {
    read_bounded_response(&mut response, model, MAX_NATIVE_IMAGE_RESPONSE_BYTES).await
}

async fn read_bounded_response(
    response: &mut Response,
    model: &str,
    max_bytes: usize,
) -> Result<Vec<u8>> {
    if let Some(content_length) = response.content_length() {
        if content_length > max_bytes as u64 {
            bail!("native Gemini response for {model} exceeds the {max_bytes}-byte safety limit");
        }
    }

    let initial_capacity = response
        .content_length()
        .and_then(|length| usize::try_from(length).ok())
        .unwrap_or(0);
    let mut body = Vec::with_capacity(initial_capacity);
    while let Some(chunk) = response
        .chunk()
        .await
        .with_context(|| format!("read native Gemini response for {model}"))?
    {
        if chunk.len() > max_bytes.saturating_sub(body.len()) {
            bail!("native Gemini response for {model} exceeds the {max_bytes}-byte safety limit");
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
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
    let tool_use_prompt = value_u64(
        usage,
        &["toolUsePromptTokenCount", "tool_use_prompt_token_count"],
    )
    .unwrap_or(0);
    // Gemini defines totalTokenCount as prompt + candidates + tool-use prompt
    // + thoughts. `candidatesTokenCount` excludes thoughts, so keep visible
    // candidate output and reasoning separate and only use this sum as the
    // fallback when a provider omits totalTokenCount.
    let derived_total = prompt_total
        .saturating_add(output_total)
        .saturating_add(tool_use_prompt)
        .saturating_add(thoughts);
    let total =
        value_u64(usage, &["totalTokenCount", "total_token_count"]).unwrap_or(derived_total);
    if prompt_total == 0
        && output_total == 0
        && cache_read == 0
        && image_input == 0
        && image_output == 0
        && audio_input == 0
        && audio_output == 0
        && thoughts == 0
        && tool_use_prompt == 0
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
        tool_use_prompt_tokens: tool_use_prompt,
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
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
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
                "toolUsePromptTokenCount": 13,
                "thoughtsTokenCount": 20,
                "totalTokenCount": 1_813,
                "costNanoUsd": settled_ndollars
            }
        })
    }

    async fn mount_native_responses(server: &MockServer, settled_ndollars: u64) {
        for (index, model) in GEMINI_IMAGE_MODELS.into_iter().enumerate() {
            mount_native_response(
                server,
                model,
                &format!("gm-request-{index}"),
                settled_ndollars,
            )
            .await;
        }
    }

    async fn mount_native_response(
        server: &MockServer,
        model: &str,
        request_id: &str,
        settled_ndollars: u64,
    ) {
        Mock::given(method("POST"))
            .and(path(format!(
                "{GENERATE_CONTENT_PATH_PREFIX}{model}{GENERATE_CONTENT_ACTION}"
            )))
            .and(header("x-goog-api-key", "buyer-secret"))
            .and(body_json(image_probe_body()))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("x-gm-request-id", request_id)
                    .set_body_json(native_response(model, request_id, settled_ndollars)),
            )
            .mount(server)
            .await;
    }

    async fn mount_native_failure(server: &MockServer, model: &str) {
        Mock::given(method("POST"))
            .and(path(format!(
                "{GENERATE_CONTENT_PATH_PREFIX}{model}{GENERATE_CONTENT_ACTION}"
            )))
            .and(header("x-goog-api-key", "buyer-secret"))
            .and(body_json(image_probe_body()))
            .respond_with(
                ResponseTemplate::new(500)
                    .set_body_string("provider-secret and GENERATED_IMAGE_MUST_NOT_BE_PRINTED"),
            )
            .mount(server)
            .await;
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
        assert_eq!(reports[0].usage.tool_use_prompt_tokens, 13);
        assert_eq!(reports[0].usage.total_tokens, 1_813);
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
        let partial = error
            .downcast_ref::<PartialCanaryFailure>()
            .expect("mismatch retains safe evidence");
        assert_eq!(partial.reports.len(), 2);
        assert!(partial
            .reports
            .iter()
            .all(|report| report.reconciliation == "mismatch"));
        assert!(partial
            .reports
            .iter()
            .all(|report| report.balance_delta_ndollars == Some(1)));
    }

    #[tokio::test]
    async fn first_success_second_failure_returns_first_reconciliation_evidence() {
        let server = MockServer::start().await;
        mount_live_offers(&server, true).await;
        mount_native_response(&server, GEMINI_IMAGE_MODELS[0], "first-request", 123).await;
        mount_native_failure(&server, GEMINI_IMAGE_MODELS[1]).await;
        mount_credit_sequence(&server, 10_000, 9_877).await;

        let error = run_image_canary_with_client(&client(), &server.uri(), "buyer-secret")
            .await
            .expect_err("a later SKU failure must make the run non-zero");
        assert!(error.to_string().contains("partial failure"));
        let partial = error
            .downcast_ref::<PartialCanaryFailure>()
            .expect("partial result retains successful evidence");
        assert_eq!(partial.reports.len(), 1);
        let report = &partial.reports[0];
        assert_eq!(report.model, GEMINI_IMAGE_MODELS[0]);
        assert_eq!(report.request_id, "first-request");
        assert_eq!(report.settled_ndollars, 123);
        assert_eq!(report.balance_before_ndollars, Some(10_000));
        assert_eq!(report.balance_after_ndollars, Some(9_877));
        assert_eq!(report.balance_delta_ndollars, Some(123));
        assert_eq!(report.reconciliation, "partial");
        let safe = serde_json::to_string(report).expect("safe partial report");
        assert!(!safe.contains("provider-secret"));
        assert!(!safe.contains("GENERATED_IMAGE_MUST_NOT_BE_PRINTED"));
    }

    #[tokio::test]
    async fn first_failure_second_success_returns_second_reconciliation_evidence() {
        let server = MockServer::start().await;
        mount_live_offers(&server, true).await;
        mount_native_failure(&server, GEMINI_IMAGE_MODELS[0]).await;
        mount_native_response(&server, GEMINI_IMAGE_MODELS[1], "second-request", 456).await;
        mount_credit_sequence(&server, 10_000, 9_544).await;

        let error = run_image_canary_with_client(&client(), &server.uri(), "buyer-secret")
            .await
            .expect_err("an earlier SKU failure must make the run non-zero");
        assert!(error.to_string().contains("partial failure"));
        let partial = error
            .downcast_ref::<PartialCanaryFailure>()
            .expect("partial result retains successful evidence");
        assert_eq!(partial.reports.len(), 1);
        let report = &partial.reports[0];
        assert_eq!(report.model, GEMINI_IMAGE_MODELS[1]);
        assert_eq!(report.request_id, "second-request");
        assert_eq!(report.settled_ndollars, 456);
        assert_eq!(report.balance_delta_ndollars, Some(456));
        assert_eq!(report.reconciliation, "partial");
    }

    #[tokio::test]
    async fn declared_oversized_native_response_is_rejected_before_body_read() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let server = tokio::spawn(async move {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let mut request = [0_u8; 1024];
            let _ = socket.read(&mut request).await;
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                MAX_NATIVE_IMAGE_RESPONSE_BYTES + 1
            );
            let _ = socket.write_all(headers.as_bytes()).await;
        });

        let response = client()
            .get(format!("http://{address}"))
            .send()
            .await
            .expect("oversized response headers");
        let error = read_bounded_native_response(response, GEMINI_IMAGE_MODELS[0])
            .await
            .expect_err("declared oversized response");
        assert!(error.to_string().contains("safety limit"), "{error:?}");
        server.await.expect("server");
    }

    #[tokio::test]
    async fn chunked_oversized_native_response_is_rejected_while_streaming() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let server = tokio::spawn(async move {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let mut request = [0_u8; 1024];
            let _ = socket.read(&mut request).await;
            let _ = socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
                )
                .await;
            let test_limit = 1024;
            let first_chunk = vec![b'x'; test_limit];
            let chunk_header = format!("{:X}\r\n", first_chunk.len());
            let _ = socket.write_all(chunk_header.as_bytes()).await;
            let _ = socket.write_all(&first_chunk).await;
            let _ = socket.write_all(b"\r\n1\r\nx\r\n0\r\n\r\n").await;
        });

        let response = client()
            .get(format!("http://{address}"))
            .send()
            .await
            .expect("chunked response headers");
        let mut response = response;
        let error = read_bounded_response(&mut response, GEMINI_IMAGE_MODELS[0], 1024)
            .await
            .expect_err("chunked oversized response");
        assert!(error.to_string().contains("safety limit"), "{error:?}");
        server.await.expect("server");
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
        assert_eq!(usage.tool_use_prompt_tokens, 13);
        assert_eq!(usage.total_tokens, 1_813);
        assert_eq!(parse_settled_ndollars(&value), Some(321));
        let report = ImageCanaryReport {
            model: "gemini-3.1-flash-image".to_owned(),
            request_id: "request-1".to_owned(),
            usage,
            settled_ndollars: 321,
            balance_before_ndollars: Some(1_000),
            balance_after_ndollars: Some(679),
            balance_delta_ndollars: Some(321),
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

    #[test]
    fn canonical_gemini_usage_separates_visible_output_from_thoughts() {
        let value = json!({
            "usageMetadata": {
                "promptTokenCount": 19,
                "candidatesTokenCount": 7,
                "toolUsePromptTokenCount": 11,
                "thoughtsTokenCount": 41
            }
        });
        let usage = parse_usage_dimensions(&value).expect("canonical usage");
        assert_eq!(usage.input_tokens, 19);
        assert_eq!(usage.output_tokens, 7);
        assert_eq!(usage.tool_use_prompt_tokens, 11);
        assert_eq!(usage.reasoning_tokens, 41);
        assert_eq!(usage.total_tokens, 78);
        assert!(usage.reasoning_tokens > usage.output_tokens);
    }
}
