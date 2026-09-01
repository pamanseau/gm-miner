# Gemini image canary

The image canary is an explicit, paid testnet check for the two native Gemini
image products:

```text
gemini/gemini-3.1-flash-lite-image
gemini/gemini-3.1-flash-image
```

It is not part of `gmcli doctor` or `gmcli check-streaming`. Routine worker
health checks stay text-only so they cannot unexpectedly generate an image.

## Run it

Use a funded GM buyer API key, not the Google provider key stored in a worker:

```sh
GM_API_KEY=<funded-testnet-buyer-key> \
  gmcli --network testnet image-canary
```

`--buyer-api-key` is also accepted. The command uses
`https://test-api.saygm.com` by default. `GM_GATEWAY_URL` or `--gateway-url`
can override that host for local/mock verification, but the command still
refuses every network except explicit testnet.

Before spending, it requests:

```text
GET /v1/models?api_shape=generateContent
```

Both exact image SKU ids must be present with `available: true`. If either
live eligible offer is missing, the command exits before reading credits or
sending either paid generation request.

If both offers are present, it sends exactly one non-streaming native request
per SKU:

```text
POST /v1beta/models/{model}:generateContent
```

The fixed private probe asks for one `IMAGE` candidate at `imageSize: "1K"`
(`candidateCount: 1`). It includes no `tools` field, so grounding/search is
not requested. The generated image and the probe prompt are never printed or
written to disk.

On success, stdout contains one JSON object per SKU with only reconciliation
data: `model`, gateway/provider `request_id`, all usage dimensions,
`settled_ndollars` (nUSD), and `balance_before_ndollars` /
`balance_after_ndollars` when `/v1/credits` is readable. If both balances are
available, the command requires their decrease to equal the sum of the two
settled charges; a mismatch exits non-zero. When balances are unavailable,
`reconciliation` is reported as `"unavailable"` rather than inventing a
balance.
