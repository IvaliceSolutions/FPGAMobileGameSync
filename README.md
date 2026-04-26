# FPGAMobileGameSync

Manual sync tooling for MiSTer FPGA and an Android RetroArch handheld, using a
Garage/S3 bucket as the intermediate store.

The first supported systems are:

- Game Boy Advance
- Super Nintendo
- Sony PlayStation

The sync model is intentionally explicit:

1. Compare the source with Netcup/Garage before uploading.
2. Upload only changed, added, renamed, moved, or deleted items.
3. Compare Netcup/Garage with the target before downloading.
4. Download only what the target needs.

Deletes are logical deletes by default. Files are moved to a trash area instead
of being hard-deleted.

## Current Status

This repository currently contains:

- `mister-thor-sync.yaml`: the human-readable sync configuration.
- `mister-thor-sync.json`: a generated JSON version for runtimes without
  PyYAML.
- `fpgms`: a small Python CLI with an initial `scan` command.

## Development Usage

From this repository:

```sh
PYTHONPATH=src python3 -m fpgmobilegamesync.cli --config mister-thor-sync.json scan --device mister
```

To scan one system/type:

```sh
PYTHONPATH=src python3 -m fpgmobilegamesync.cli --config mister-thor-sync.json scan --device thor --system gba --type saves
```

The command prints a JSON manifest with paths, sizes, modification times, and
SHA-256 hashes.

Each scanned item contains:

- `relative_path`: path relative to the device root.
- `content_path`: path relative to the configured content folder.
- `sync_key`: canonical object key under the S3 `systems/` prefix.

`content_path` is what lets MiSTer and Thor be compared even though their local
roots differ.

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

The future sync engine will use these manifests to detect:

- unchanged files
- modified files
- renames
- moves
- folder renames, especially PSX game folders
- additions
- logical deletions
- conflicts
