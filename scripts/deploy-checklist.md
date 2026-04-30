# Deploy checklist

Lightweight runbook for promoting changes to the production daemon host. Keep
it short on purpose — the goal is to make the production overlay pattern
discoverable, not to replicate full ops docs.

## Production config overlay

`config.yml` is committed and tracked by git. It holds the defaults that travel
with the repo. `config.production.yml` is a sibling overlay file that lives
**only on the production host** and is gitignored. The daemon loads
`config.yml` first and then deep-merges `config.production.yml` on top:

- nested mappings merge recursively (e.g. `daemon.fix_iteration_cap` in the
  overlay only changes that field; the rest of `daemon:` stays at base values);
- lists in the overlay replace lists in the base (no concat);
- unknown keys in the overlay are logged as warnings and ignored, so
  forward-compatible fields are safe to drop in early.

The overlay survives `git reset --hard`, so a deploy that resets the working
tree no longer reverts production daemon settings to upstream defaults.

### Bootstrapping the overlay on a new host

1. Copy the template: `cp config.production.example.yml config.production.yml`.
2. Edit `config.production.yml` to keep only the keys you intend to override.
   Delete every other key — the base values come from `config.yml` and there
   is no need to mirror them.
3. Restart the daemon. On startup the daemon logs:
   `Applied config.production.yml overlay fields: …` listing the dotted paths
   that were applied.

### Verifying the running daemon matches expected config

```bash
diff <(yq . config.yml) \
     <(docker compose exec daemon python -c \
       "import json; from src.config import load_config; \
        print(json.dumps(load_config().model_dump(), default=str))")
```

A non-empty diff is expected when the overlay is in use — the right-hand side
is the merged view. Spot-check that each overridden field matches your
intent.

### Updating overrides safely

- Edit `config.production.yml` directly on the host; do not commit it.
- The daemon's hot-reload watches `config.yml` for changes; if you rely on
  hot-reload you may also need to bump `config.yml` (e.g. touch its mtime)
  after editing the overlay.
- Keep `config.production.example.yml` in sync when introducing new
  production-only overrides so the next host bootstrap is obvious.

## Other deploy reminders

- Do not `git add` `config.production.yml` (the `.gitignore` already excludes
  it; resist the urge to `-f`).
- Keep the example file (`config.production.example.yml`) up to date when you
  add fields in `src/config.py` that production typically overrides.
