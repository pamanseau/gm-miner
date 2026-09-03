# gm-miner — KubeTEE operator notes

`CLAUDE.md` is upstream (`taostat/gm-miner`). Keep KubeTEE-only
guidance here so a submodule update does not clobber it.

## Phala rollout (replace a live CVM)

**Start the new instance first. Delete the old one only after the new worker is up.**

`deploy` against the live `--app-name` will refuse (name collision) and print
`phala cvms delete`. Do **not** follow that hint while the old CVM is still the
serving worker: tearing it down first leaves the hotkey with `failed_attestation`
and no eligible offers until the replacement boots. `gmcli deploy` is only for
worker #1 on a fresh hotkey; a second (or replacement) instance is always
`worker add` with a **new** `--app-name`.

```bash
# 1. Record the live worker (old worker_id + Phala app_id / app-name).
gmcli worker list
phala cvms ls

# 2. Boot a new CVM under an unused name. Same flags as the live worker
#    (instance type, digest-pinned image, boot timeout, network).
gmcli worker add --app-name gm-miner-2 --yes --accept-terms \
  --network mainnet \
  --instance-type tdx.large \
  --image-ref ghcr.io/taostat/gm-miner@sha256:<approved> \
  --boot-timeout-secs 600

# 3. Wait until ALL are true — not merely "CVM created":
#    - phala cvms get <new_app_id> is running
#    - gmcli worker list shows the new worker active (attestation passed)
#    - gmcli status shows the hotkey's offers eligible
gmcli worker list
gmcli status

# 4. Only then tear down the old instance. Order matters:
#    CVM first (stops billing / the TEE), then registry.
phala cvms delete <old_app_id> --yes
gmcli worker remove <old_worker_id>
```

`worker remove` deregisters the registry row only — it does **not** delete the
Phala CVM. Do not `worker remove` the old worker before the new one is `active`.
Do not reuse the live `--app-name` to "upgrade in place."

`phala deploy` cannot reuse a CVM name, so `deploy` / `worker add` probe for an
existing CVM under `--app-name` and stop with the `phala cvms delete <app_id>`
to run. That message is a **name collision**, not an upgrade procedure. gmcli
never deletes a CVM: that destroys a running worker and stays the operator's
explicit act. To replace a live instance, start a **new** CVM first — never
delete the old one to free the name.

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
after ~60s before suspecting a real problem:

```bash
curl -sk -m 60 <endpoint>-8080s.dstack-pha-prodX.phala.network/v1/chat/completions \
  -H "x-gm-node-key: <GM_NODE_SECRET from dist/<app>/.env>" \
  -H "Authorization: Bearer <KUBETEE_API_KEY from dist/<app>/.env>" \
  -H "content-type: application/json" \
  -H "x-gm-provider: kubetee" \
  -d '{"model":"z-ai/glm-5.3","messages":[{"role":"user","content":"say ok"}],"max_tokens":5}'
```

A new worker may also show `failed_attestation` on its first registry cycle
(boot race) — the next cycle flips it to `active`.
