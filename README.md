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

For PSX, keep both names in metadata:

- `mister_game_folder`: the MiSTer folder containing the game files.
- `retroarch_game_file`: the RetroArch game file, usually `.chd`, `.cue`, or
  `.m3u`.

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
2. Otherwise, look for `CD 1`.
3. Then look for `CD1`.
4. Finally, look for an isolated `1`.

If the first disc cannot be inferred unambiguously, conversion stops and asks
for `--retroarch-game-file`.

The future sync engine will use these manifests to detect:

- unchanged files
- modified files
- renames
- moves
- folder renames, especially PSX game folders
- additions
- logical deletions
- conflicts
