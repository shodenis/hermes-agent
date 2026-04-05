# Fork maintenance — Hermes Agent (production)

This repository tracks **[NousResearch/hermes-agent](https://github.com/NousResearch/hermes-agent)** with a small set of **production customizations** maintained on branch **`prod`**.

## Branches

| Branch | Purpose |
|--------|---------|
| **`upstream-tracking`** | Local branch reset to **`upstream/main`** after each fetch. Read-only pointer for diffs and merges — do not commit here. |
| **`prod`** | **Production branch**: `upstream/main` + fork commits (MAX platform, email sanitizer, related gateway changes). Services run from this branch. |
| **`main`** | Optional; may match an older layout. **Production uses `prod`.** |

Update `upstream-tracking` before comparing or merging:

```bash
git fetch upstream
git branch -f upstream-tracking upstream/main
```

## What we fork

1. **MAX (VK Teams / max.ru)** — `gateway/platforms/max.py`, gateway registration in `gateway/run.py`, `Platform.MAX` in `gateway/config.py`, `send_message` extensions.
2. **Outbound email sanitizer** — `gateway/platforms/email.py` strips problematic content before SMTP.
3. **Merge resolutions** — Prior cherry-picks were squashed into `feat(fork): MAX platform…`; email fix is `fix(fork): sanitize outbound email…`.

Regenerate patches any time `prod` changes relative to `upstream-tracking`:

```bash
cd /root/.hermes/hermes-agent
mkdir -p /root/.hermes/hooks/post-update/patches
git diff upstream-tracking..prod -- gateway/config.py   > /root/.hermes/hooks/post-update/patches/10-config-max.patch
git diff upstream-tracking..prod -- gateway/run.py      > /root/.hermes/hooks/post-update/patches/20-run-max.patch
git diff upstream-tracking..prod -- gateway/platforms/max.py   > /root/.hermes/hooks/post-update/patches/30-max-py.patch
git diff upstream-tracking..prod -- gateway/platforms/email.py > /root/.hermes/hooks/post-update/patches/40-email-sanitize.patch
```

## Patch pipeline (optional re-apply)

Patches are a **replay aid** if you ever need to re-apply the same edits onto a clean upstream tree (e.g. new clone or conflict recovery). They are **not** run automatically by `hermes-auto-update.sh` (that script **merges** `upstream-tracking` into `prod`).

| Patch | File |
|-------|------|
| `10-config-max.patch` | `gateway/config.py` |
| `20-run-max.patch` | `gateway/run.py` |
| `30-max-py.patch` | `gateway/platforms/max.py` |
| `40-email-sanitize.patch` | `gateway/platforms/email.py` |

Apply in order:

```bash
/root/.hermes/hooks/post-update/apply.sh
```

Override paths if needed:

```bash
HERMES_AGENT_REPO=/path/to/hermes-agent HERMES_PATCH_DIR=/path/to/patches /root/.hermes/hooks/post-update/apply.sh
```

The script uses `patch -p1 --dry-run` first; if hunks are already present it prints `Already applied or conflict` and continues.

## Automated upstream sync

**`/root/hermes-auto-update.sh`** (cron: daily 04:00):

1. `git fetch upstream`
2. `git branch -f upstream-tracking upstream/main`
3. `git checkout prod`
4. `git merge upstream-tracking --no-edit`
5. If `HEAD` moved: `pip install -e .` in `venv`, restart `hermes.service` and `hermes-bitrix.service`, append to `/root/hermes-update.log`

**Merge conflicts:** resolve in the repo, commit, then ensure services still start. Regenerate patches after fixing `prod`.

**Cron backup:** keep a copy of the crontab when changing it, e.g. `crontab -l > /tmp/crontab-backup.txt`.

## Runtime layout

- **Repo:** `/root/.hermes/hermes-agent` (branch **`prod`**)
- **Patches:** `/root/.hermes/hooks/post-update/patches/`
- **Bitrix profile:** `/root/.hermes/profiles/bitrix/` (`HERMES_HOME` for that gateway)

## Honcho / memory

Honcho is configured via the upstream **memory provider** plugin (`memory.provider: honcho`, `honcho.json`). Gateway-local Honcho managers were removed upstream; do not reintroduce orphaned Honcho blocks when merging. See `plugins/memory/honcho/client.py` for `honcho.json` path rules (`HERMES_HOME` + fallback to `~/.hermes/honcho.json`).

## Quick checks after an update

```bash
systemctl status hermes.service hermes-bitrix.service | grep Active:
journalctl -u hermes-bitrix --since "10 min ago" | grep -E "ERROR|Exception|Traceback"
```

---

*Last aligned with production workflow: `prod` = `upstream/main` + squashed MAX + email sanitizer; patch files generated from `git diff upstream-tracking..prod`.*
