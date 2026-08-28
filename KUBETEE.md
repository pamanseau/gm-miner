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
