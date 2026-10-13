# Local P3 patch inventory

This directory is a source copy of `apalis-mysql 1.0.0-rc.8` from crates.io
(published checksum `d7e4f377b5ee7a5bb275b5d1d4494d09e9789a52c2ea4e96dd10e1c1f6a0fbf9`).
The package version is intentionally unchanged so the workspace's exact
compatibility lock remains effective. The root `[patch.crates-io]` entry is the
only dependency override.

Repository-specific changes:

- increment `jobs.claim_generation` during fetch and carry the value in task extensions;
- require `id + lock_by + claim_generation` for lock and acknowledgement;
- write retry backoff, result, attempt count, completion time, exit evidence and terminal origin in one acknowledgement statement;
- increment the generation when the upstream orphan requeue path revokes a claim;
- expose JSON and pre-encoded task insertion functions that use a caller-owned MySQL transaction;
- replace compile-time `query_file!` calls with bound runtime queries so the vendored crate builds in restricted Windows and CI workspaces;
- add isolated-MySQL protocol tests prefixed `p3_`.

The fork does not run its bundled migrations in the application. Schema remains
owned by `simple-base-db/scheduler.sql` and numbered repository migrations.
