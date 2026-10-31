# Deploy checklist

Use the single [release and rollback procedure](../docs/operations.md#release-and-rollback-procedure)
for every release. This is an operator checklist, not authorization to deploy.

- [ ] Record the exact candidate and prior running source SHAs, image IDs,
      checkout, Compose project, config identities and required CI/review evidence.
- [ ] Preserve the current user pause; drain and confirm every process owner.
- [ ] Preserve local config and verify backup scope against the PR-434 inventory;
      a Git bundle is not a complete operational backup.
- [ ] Choose source-mounted recreation or image rebuild explicitly, and review
      rollback state/schema compatibility before changing source.
- [ ] Recreate only the intended services, record container/image/source evidence,
      and report missing startup identity/readiness proof explicitly.
- [ ] Perform only the separately authorized bounded smoke; record exact terminal
      outcomes, failures and remaining unverified boundaries. Keep pause in place.

## Production config overlay

`config.yml` is tracked defaults. The sibling ignored `config.production.yml`
overrides scalars and lists and recursively merges mappings in `src/config.py`.
Unknown overlay keys are warned about and ignored. `PO_CONFIG_PATH` can select a
different base; supported environment overrides also affect effective settings.

On a **new host only**, create the overlay from
`config.production.example.yml` after confirming no overlay exists; edit it to
contain only intended overrides. Never copy the example over an existing file.
Preserve both files and environment references before release; never commit
credentials or force-add the overlay. Keep the example current when adding fields.

For override updates, use the canonical procedure's config validation and drain
boundary. The daemon watches the base config and also periodically reloads;
coder-sensitive changes may be deferred. A fresh `load_config()` process proves
parsing of current files, not the configuration adopted by an existing runner.
