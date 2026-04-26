# FPGAMobileGameSync

Manual sync tooling for MiSTer FPGA and an Android RetroArch handheld, using a
Garage/S3 bucket as the intermediate store.

The first supported systems are:

- Game Boy Advance via RetroArch mGBA
- Super Nintendo via RetroArch Mesen-S
- Sony PlayStation via RetroArch SwanStation

The sync model is intentionally explicit:

1. Compare the source with Netcup/Garage before uploading.
2. Upload only changed, added, renamed, moved, or deleted items.
3. Compare Netcup/Garage with the target before downloading.
4. Download only what the target needs.

Deletes are logical deletes by default. Files are moved to a trash area instead
of being hard-deleted.

The sync engine is controller-agnostic. It may run on MiSTer, on the AYN Thor,
or on a third device, as long as the controller can reach both endpoints and the
Garage/S3 bucket over the network.

## Current Status

This repository currently contains:

- `mister-thor-sync.yaml`: the human-readable sync configuration.
- `mister-thor-sync.json`: a generated JSON version for runtimes without
  PyYAML.
- `fpgms`: a Python CLI for scanning, comparing, planning, applying local-store
  plans, and converting saves.

## Development Usage

From this repository:

```sh
PYTHONPATH=src python3 -m fpgmobilegamesync.cli --config mister-thor-sync.json scan --device mister
```

Before a real run, check the configuration without changing files:

```sh
PYTHONPATH=src python3 -m fpgmobilegamesync.cli --config mister-thor-sync.json doctor \
  --backend s3 \
  --pretty
```

Add `--check-paths` when the MiSTer/Thor storage is mounted or when running on
one of the devices:

```sh
PYTHONPATH=src python3 -m fpgmobilegamesync.cli --config mister-thor-sync.json doctor \
  --backend s3 \
  --check-paths \
  --pretty
```

Add `--check-remote` before using the configured SFTP access from a third
controller device:

```sh
PYTHONPATH=src python3 -m fpgmobilegamesync.cli --config mister-thor-sync.json doctor \
  --backend s3 \
  --check-remote \
  --pretty
```

To scan one system/type:

```sh
PYTHONPATH=src python3 -m fpgmobilegamesync.cli --config mister-thor-sync.json scan --device thor --system gba --type saves
```

To scan through the configured SFTP access instead of mounted local paths,
install the optional dependency and switch the scan backend:

```sh
python3 -m pip install ".[sftp]"
PYTHONPATH=src python3 -m fpgmobilegamesync.cli --config mister-thor-sync.json scan \
  --backend sftp \
  --device mister \
  --system gba \
  --type saves \
  --pretty
```

The command prints a JSON manifest with paths, sizes, modification times, and
SHA-256 hashes.

Each scanned item contains:

- `relative_path`: path relative to the device root.
- `content_path`: canonical path used for comparison and object-store keys.
- `native_content_path`: actual path relative to the configured content folder
  on that device.
- `sync_key`: canonical object key under the S3 `systems/` prefix.
- `canonical_sha256` / `canonical_size`: identity used for sync decisions.
- `native_sha256` / `native_size`: raw local file fingerprint used for apply-time
  safety checks.

`content_path` is what lets MiSTer and Thor be compared even though their local
roots and save extensions differ. For example, Thor `Golden Sun.srm` and MiSTer
`Golden Sun.sav` both map to canonical `Golden Sun.sav`, while
`native_content_path` preserves the real local filename to use during apply.

For GBA and SNES the canonical and native hashes are identical. For PSX they are
kept separate because the future PSX converter may produce different native
bytes per target while still representing the same per-game save.

## Comparing Manifests

Compare two scan manifests:

```sh
PYTHONPATH=src python3 -m fpgmobilegamesync.cli compare \
  --source source-manifest.json \
  --target target-manifest.json \
  --source-name mister \
  --target-name s3 \
  --pretty
```

The comparator detects:

- `unchanged`
- `modified`
- `renamed`
- `moved`
- `renamed_moved`
- `added`
- `deleted`
- `ambiguous_rename`

## Planning Actions

Build an upload plan:

```sh
PYTHONPATH=src python3 -m fpgmobilegamesync.cli plan \
  --mode upload \
  --source mister-manifest.json \
  --target s3-manifest.json \
  --source-name mister \
  --target-name s3 \
  --pretty
```

Build a download plan:

```sh
PYTHONPATH=src python3 -m fpgmobilegamesync.cli plan \
  --mode download \
  --source s3-manifest.json \
  --target thor-manifest.json \
  --source-name s3 \
  --target-name thor \
  --pretty
```

Plan operations are dry-run oriented and currently include:

- `noop`
- `upload`
- `download`
- `rename_remote`
- `rename_local`
- `trash_remote`
- `trash_local`
- `conflict`

## Full Sync Workflow

The `sync` command runs the two required comparison/apply phases in order:

1. scan source
2. scan S3/store
3. plan source -> S3/store
4. optionally apply source -> S3/store
5. rescan S3/store
6. scan target
7. plan S3/store -> target
8. optionally apply S3/store -> target

The command is dry-run by default:

```sh
PYTHONPATH=src python3 -m fpgmobilegamesync.cli sync \
  --direction mister-to-thor \
  --backend local \
  --store-root /tmp/fpgms-store \
  --report-dir /tmp/fpgms-runs/mister-to-thor-dry-run \
  --system gba \
  --type saves \
  --pretty
```

Add `--apply` to write changes:

```sh
PYTHONPATH=src python3 -m fpgmobilegamesync.cli sync \
  --direction mister-to-thor \
  --backend local \
  --store-root /tmp/fpgms-store \
  --report-dir /tmp/fpgms-runs/mister-to-thor-apply \
  --system gba \
  --type saves \
  --apply \
  --pretty
```

Use Garage/S3 directly by switching the backend. The S3 configuration is read
from the main config file, so `--store-root` is only needed for `--backend local`.
When `--apply` is used, the S3 workflow takes a short-lived `locks/sync.json`
lock before scanning and writing. This prevents two controllers from applying
changes to the same Garage bucket at the same time; the lock is released at the
end of the run and its token is omitted from reports:

```sh
PYTHONPATH=src python3 -m fpgmobilegamesync.cli sync \
  --direction mister-to-thor \
  --backend s3 \
  --report-dir /tmp/fpgms-runs/mister-to-thor-s3 \
  --system gba \
  --type saves \
  --apply \
  --pretty
```

The default lock TTL is 30 minutes. Use `--lock-ttl-seconds` to change it, and
reserve `--no-lock` for recovery/debugging when you have already verified that
no other sync is running.

From a third controller device, use the configured SFTP blocks for MiSTer and
Thor by adding `--scan-backend sftp`. This scans both devices remotely, uploads
source changes to Garage/S3, then downloads target changes back over SFTP:

```sh
PYTHONPATH=src python3 -m fpgmobilegamesync.cli sync \
  --direction mister-to-thor \
  --backend s3 \
  --scan-backend sftp \
  --report-dir /tmp/fpgms-runs/mister-to-thor-sftp \
  --system gba \
  --type saves \
  --apply \
  --pretty
```

When running directly on one of the two devices, use a mixed backend: `local`
for the controller device and `sftp` for the other one.

From the Thor, pull MiSTer changes into the local Thor filesystem:

```sh
PYTHONPATH=src python3 -m fpgmobilegamesync.cli sync \
  --direction mister-to-thor \
  --backend s3 \
  --source-backend sftp \
  --target-backend local \
  --report-dir /tmp/fpgms-runs/mister-to-thor-from-thor \
  --system gba \
  --type saves \
  --apply \
  --pretty
```

From the Thor, push local Thor changes back to MiSTer:

```sh
PYTHONPATH=src python3 -m fpgmobilegamesync.cli sync \
  --direction thor-to-mister \
  --backend s3 \
  --source-backend local \
  --target-backend sftp \
  --report-dir /tmp/fpgms-runs/thor-to-mister-from-thor \
  --system gba \
  --type saves \
  --apply \
  --pretty
```

Use the opposite pairing when running on MiSTer: `--source-backend local
--target-backend sftp` for `mister-to-thor`, and `--source-backend sftp
--target-backend local` for `thor-to-mister`.

For development, mounted shares, or a third controller device, override the
configured source and target roots:

```sh
PYTHONPATH=src python3 -m fpgmobilegamesync.cli sync \
  --direction thor-to-mister \
  --backend local \
  --store-root /tmp/fpgms-store \
  --report-dir /tmp/fpgms-runs/thor-to-mister \
  --source-root /Volumes/thor-storage \
  --target-root /Volumes/mister-fat \
  --system gba \
  --type saves \
  --apply \
  --pretty
```

With the local backend, `--source-root` and `--target-root` may be real local
paths, mounted network shares, or device filesystems exposed by another tool.
With the S3 backend, the same overrides let a third controller sync two mounted
devices through Garage/S3. With `--scan-backend sftp`, the configured remote
roots are used instead.

When `--report-dir` is provided, the command writes:

- `source-manifest.json`
- `store-before-upload-manifest.json`
- `upload-plan.json`
- `upload-apply.json` when `--apply` is used
- `store-after-upload-manifest.json`
- `target-manifest.json`
- `download-plan.json`
- `download-apply.json` when `--apply` is used
- `summary.json`

## Local Object Store

A filesystem-backed object store is available to simulate the future S3/Garage
bucket without touching the network:

```sh
PYTHONPATH=src python3 -m fpgmobilegamesync.cli store scan \
  --root /tmp/fpgms-store \
  --pretty
```

It scans objects under this layout:

```text
systems/<system>/<type>/<content-path>
```

For example:

```text
/tmp/fpgms-store/systems/gba/games/Metroid Fusion.gba
/tmp/fpgms-store/systems/snes/saves/Chrono Trigger.srm
```

Trash objects are moved under:

```text
trash/<timestamp-utc>/<origin-device>/systems/...
```

List logical deletes:

```sh
PYTHONPATH=src python3 -m fpgmobilegamesync.cli store trash list \
  --root /tmp/fpgms-store \
  --pretty
```

Restore one trash entry to its original sync key:

```sh
PYTHONPATH=src python3 -m fpgmobilegamesync.cli store trash restore \
  --root /tmp/fpgms-store \
  --trash-key "trash/2026-04-26T20-00-00Z/mister/systems/gba/saves/Golden Sun.sav" \
  --pretty
```

Restore refuses to replace an existing object unless `--overwrite` is passed.
When overwriting, the replaced object is copied under `backups/` first.

## Garage/S3 Store Inspection

The real Garage/S3 backend reads its connection settings from
`mister-thor-sync.json` and secrets from environment variables:

```sh
export MISTER_THOR_S3_ENDPOINT_URL="https://garage.example"
export MISTER_THOR_S3_ACCESS_KEY_ID="..."
export MISTER_THOR_S3_SECRET_ACCESS_KEY="..."
```

Install the optional S3 dependency before using this backend:

```sh
python3 -m pip install ".[s3]"
```

Scan currently reads the stored `manifests/s3.json` file instead of downloading
and hashing every object in the bucket:

```sh
PYTHONPATH=src python3 -m fpgmobilegamesync.cli store scan \
  --backend s3 \
  --pretty
```

List and restore logical deletes on Garage/S3:

```sh
PYTHONPATH=src python3 -m fpgmobilegamesync.cli store trash list \
  --backend s3 \
  --pretty
```

```sh
PYTHONPATH=src python3 -m fpgmobilegamesync.cli store trash restore \
  --backend s3 \
  --trash-key "trash/2026-04-26T20-00-00Z/mister/systems/gba/saves/Golden Sun.sav" \
  --pretty
```

As with the local backend, restore refuses to overwrite unless `--overwrite` is
passed, and overwritten objects are copied under `backups/` first.

Inspect S3 sync locks if a previous `sync --backend s3 --apply` was interrupted:

```sh
PYTHONPATH=src python3 -m fpgmobilegamesync.cli store lock list \
  --backend s3 \
  --pretty
```

An expired lock can be cleared directly. Use `--force` only after confirming no
other controller is currently syncing:

```sh
PYTHONPATH=src python3 -m fpgmobilegamesync.cli store lock clear \
  --backend s3 \
  --name sync \
  --force \
  --pretty
```

Apply an upload/remote plan directly to Garage/S3:

```sh
PYTHONPATH=src python3 -m fpgmobilegamesync.cli apply \
  --plan upload-plan.json \
  --backend s3 \
  --pretty
```

This supports `upload`, `rename_remote`, `trash_remote`, `noop`, and refused or
skipped `conflict` operations. Before overwriting, renaming, or trashing an
existing object, the S3 backend re-reads the object and verifies the
`native_sha256` / `native_size` stored in the plan. After applying, it rebuilds
and writes `manifests/s3.json` from the current `systems/` objects.

Apply an S3 -> local target download plan:

```sh
PYTHONPATH=src python3 -m fpgmobilegamesync.cli apply \
  --plan download-plan.json \
  --backend s3 \
  --target-root /Volumes/thor-storage/RetroArch/saves/GBA \
  --trash-root /Volumes/thor-storage/RetroArch/.sync_trash \
  --pretty
```

For download plans, the S3 backend stages each source object in a temporary
file, verifies it against the plan fingerprint, then reuses the same local
target logic as the filesystem backend. That means local backups, local trash,
case-only renames, and save conversion still follow the same rules.

## Applying A Plan To The Local Object Store

Apply a plan against the local object-store backend:

```sh
PYTHONPATH=src python3 -m fpgmobilegamesync.cli apply \
  --plan upload-plan.json \
  --backend local \
  --store-root /tmp/fpgms-store \
  --pretty
```

The local backend currently supports remote-side operations:

- `noop`
- `upload`
- `rename_remote`
- `trash_remote`
- `conflict`, which is refused unless `--allow-conflicts` is passed

For download plans, apply to a local target root:

```sh
PYTHONPATH=src python3 -m fpgmobilegamesync.cli apply \
  --plan download-plan.json \
  --backend local \
  --target-root /tmp/fpgms-target \
  --trash-root /tmp/fpgms-target/.sync_trash \
  --pretty
```

Local target apply supports:

- `noop`
- `download`
- `rename_local`
- `trash_local`
- `conflict`, which is refused unless `--allow-conflicts` is passed

Before applying any write, rename, overwrite, or trash operation, the executor
rechecks the relevant `native_sha256` / `native_size` from the plan. If the
source or target changed after the plan was created, apply stops with a conflict
style error instead of overwriting or trashing stale data. For converted saves,
the produced `canonical_sha256` / `canonical_size` must also match the source
manifest before the file is written.

When an upload overwrites an existing object, the old object is copied under:

```text
backups/<timestamp-utc>/<origin-device>/systems/...
```

When a download overwrites an existing file, the old local file is copied under:

```text
<trash-root>/backups/<timestamp-utc>/<origin-device>/...
```

Local deletes are moved under:

```text
<trash-root>/deleted/<timestamp-utc>/<origin-device>/...
```

Trash and backup paths can be made deterministic for tests:

```sh
PYTHONPATH=src python3 -m fpgmobilegamesync.cli apply \
  --plan upload-plan.json \
  --backend local \
  --store-root /tmp/fpgms-store \
  --timestamp-utc 2026-04-26T20-30-00Z
```

## Runtime Notes

YAML loading requires `PyYAML`. When it is not available, use the generated
`mister-thor-sync.json` file.

## Save Conversion

Save conversion is currently implemented as safe copy plus validation for the
three supported systems:

- GBA mGBA: raw save content, MiSTer `.sav` to RetroArch `.srm`.
- SNES Mesen-S: raw save content, MiSTer `.sav` or `.srm` to RetroArch `.srm`.
- PSX SwanStation: raw 128 KiB memory card, MiSTer `.sav` or `.mcd` to
  RetroArch `.srm`.

Convert a MiSTer GBA save:

```sh
PYTHONPATH=src python3 -m fpgmobilegamesync.cli convert-save \
  --system gba \
  --direction mister-to-thor \
  --source "/path/to/Golden Sun (FR).sav" \
  --output /tmp/out
```

Inspect a PSX save before conversion:

```sh
PYTHONPATH=src python3 -m fpgmobilegamesync.cli inspect-save \
  --system psx \
  --source "/path/to/Final Fantasy 9 (FR).sav" \
  --pretty
```

For PSX, keep both names in metadata:

- `mister_game_folder`: the MiSTer folder containing the game files.
- `retroarch_game_file`: the RetroArch game file name or full path. Its
  extension can be unknown; only known PSX extensions are stripped when deriving
  the save stem.
- `retroarch_game_file_stem`: the exact save stem to use on Thor/SwanStation.

When converting to Thor/SwanStation, the save name uses the RetroArch game file
stem, not the MiSTer folder name:

```sh
PYTHONPATH=src python3 -m fpgmobilegamesync.cli convert-save \
  --system psx \
  --direction mister-to-thor \
  --source "/path/to/Final Fantasy 9 (FR).sav" \
  --output /tmp/out \
  --mister-game-folder "/media/fat/games/PSX/Final Fantasy 9 (FR)" \
  --retroarch-game-file "/storage/emulated/0/RetroArch/games/PSX/Final Fantasy IX.chd"
```

This writes:

```text
/tmp/out/Final Fantasy IX.srm
```

If `--retroarch-game-file` is not provided but `--mister-game-folder` points to
an accessible PSX folder, the converter tries to infer the game file:

1. If there is only one `.iso`, `.bin`, `.chd`, `.cue`, or `.m3u`, use it.
2. Otherwise, look for `Disc 1`.
3. Then look for `Disc1`.
4. Then look for `1 of`.
5. Then look for `CD 1`.
6. Then look for `CD1`.
7. Finally, look for an isolated `1`.

If the first disc cannot be inferred unambiguously, conversion stops and asks
for `--retroarch-game-file`.

For PSX names that differ between MiSTer and RetroArch/SwanStation, add a
mapping under `save_mappings.psx`:

```yaml
save_mappings:
  psx:
    - mister_game_folder: "Final Fantasy 9 (FR)"
      retroarch_game_file: "Final Fantasy IX.chd"
```

The extension may be omitted or unknown there too. The sync key remains based on
`mister_game_folder`; Thor output uses the RetroArch stem.

This project assumes PSX saves are per-game files in your setup, not shared
multi-game memory cards. A PSX save is therefore synchronized as one logical
file per game instead of as individual internal memory-card blocks.

PSX conversion is format-aware: the tool validates a 128 KiB raw PlayStation
memory card, checks the `MC` header and system/directory frame checksums, then
writes a target raw memory-card file with the native target name. For the
MiSTer `.sav` and RetroArch/SwanStation `.srm` path currently targeted here, the
payload is raw-compatible, but the conversion path still parses and validates
the full card instead of blindly renaming.

Names are compared case-insensitively for safety, but the source casing remains
canonical. A case-only rename such as `pokemon.sav` -> `Pokemon.sav` is planned
as a rename, and if the file content also changed it is backed up, renamed, then
overwritten with the source file.

The future sync engine will use these manifests to detect:

- unchanged files
- modified files
- renames
- moves
- folder renames, especially PSX game folders
- additions
- logical deletions
- conflicts
