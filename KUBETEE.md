# gm-miner — KubeTEE operator notes

`CLAUDE.md` is upstream (`taostat/gm-miner`). Keep KubeTEE-only
guidance here so a submodule update does not clobber it.

## Phala rollout (serial — one CVM at a time)

Roll the fleet **one CVM at a time**: delete one, start its replacement,
verify it, and only then move to the next. With every other worker still
active and serving, each step is outage-free — deleting more than one CVM
before its replacement is verified is not.

Per CVM, in order:

```bash
# 1. Delete the OLD instance. CVM first (stops billing / the TEE),
#    then the registry row.
phala cvms delete <old_app_id> --yes
gmcli worker remove <old_worker_id>
# `worker remove` deregisters the registry row AND drops the local record —
# that is what frees the --app-name and lets the re-add mint a fresh node
# secret (envoy, registry, and gateway all start from the same new value).

# 2. Start the replacement under the SAME app-name, same flags as the fleet.
gmcli worker add --app-name gm-miner-0 --yes --accept-terms \
  --network mainnet \
  --instance-type tdx.medium \
  --disk-size 40G \
  --os-image dstack-0.5.9 \
  --boot-timeout-secs 600
# Name reuse is safe because step 1 already removed the old CVM and its
# worker record, so gmcli's name-collision preflight passes. (gmcli deploy /
# worker add stopping with "phala cvms delete <app_id>" means step 1 was
# missed for that name — it is a preflight, not an upgrade procedure; gmcli
# never deletes a CVM itself.)

# 3. Verify ALL before touching the next CVM:
#    a. phala cvms get <new_app_id> → running
#    b. gmcli worker list → the new worker active (attestation passed).
#       A first failed_attestation cycle is a boot race — wait one cycle.
#    c. gm-miner is actually serving — smoke the data plane (curl below).
#    d. gmcli status → the hotkey's offers eligible.
phala cvms get <new_app_id>
gmcli worker list
gmcli status

# 4. Repeat 1–3 for the next CVM until the whole fleet is rolled.
```

Data-plane smoke test for (c) — needs the fresh node secret from the
re-rendered `dist/<app>/.env` and the `x-gm-provider` routing header:

```bash
curl -sk -m 60 <endpoint>-8080s.dstack-pha-prodX.phala.network/v1/chat/completions \
  -H "x-gm-node-key: <GM_NODE_SECRET from dist/<app>/.env>" \
  -H "Authorization: Bearer <KUBETEE_API_KEY from dist/<app>/.env>" \
  -H "content-type: application/json" \
  -H "x-gm-provider: kubetee" \
  -d '{"model":"z-ai/glm-5.3","messages":[{"role":"user","content":"say ok"}],"max_tokens":5}'
```

`gmcli deploy` is only for worker #1 on a fresh hotkey; every later (or
replacement) instance is `worker add`. Do not attempt to "upgrade in
place" against a running CVM.

## KubeTEE upstream: HTTP/2 (stock envoy config is correct)

`llm.kubetee.ai` now serves **HTTP/2**. Upstream's stock `image/envoy.yaml`
configures the `kubetee` cluster with `http2_protocol_options`, which is the
correct shape — verified working on the v0.4.14 fleet (gm-miner-0/1/2, 2026-09-03:
capability probes + chat completions all HTTP 200).

The old fork branch `fix/kubetee-http1-upstream` (`793e3c7`, "Force HTTP/1.1
for the KubeTEE upstream cluster") was a workaround for the pre-HTTP/2
Traefik TCP-passthrough + uvicorn TLS termination, which advertised no ALPN
h2. **It is obsolete — do not re-apply it.** Upstream's `http2_protocol_options`
is the right config now. (The `tls_minimum_protocol_version: TLSv1_3` pin in
the stock config also stays.)

## Deploy-time streaming self-test failures are boot timing

`gmcli worker add`'s advisory streaming self-test probes the new CVM seconds
after creation, before envoy has settled on a fresh boot. `[!!] kubetee/...:
check failed` on every route at deploy time is expected; re-probe manually
with the smoke test above after ~60s before suspecting a real problem.

A new worker may also show `failed_attestation` on its first registry cycle
(boot race) — the next cycle flips it to `active`.
