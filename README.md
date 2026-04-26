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
