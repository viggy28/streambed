# Integration test object-storage image

Streambed uses [PGSTY Silo](https://github.com/pgsty/silo), a maintained MinIO-compatible fork, for local, integration, and simulation environments. Silo keeps the S3 API and `MINIO_*` configuration used by Streambed, publishes images for `linux/amd64` and `linux/arm64`, and bundles a native readiness command plus `mcli` for bucket creation. This lets the server and bootstrap job use one dependency instead of an additional client image.

The Compose files pin a readable release tag **and** its immutable multi-platform image-index digest. The tag explains the selected version; the digest prevents a registry change from silently changing CI.

## Updating Silo

1. Review the [Silo releases](https://github.com/pgsty/silo/releases), [security policy](https://github.com/pgsty/silo/security/policy), and release notes for compatibility or security changes. Subscribe to repository releases and security advisories so the pin is reviewed when a fix is published.
2. Inspect the candidate release and record its multi-platform index digest:

   ```bash
   docker buildx imagetools inspect pgsty/silo:<release>
   ```

   Confirm that the index contains both `linux/amd64` and `linux/arm64`. Use the top-level `Digest`, not either platform-specific digest.
3. Update the identical `pgsty/silo:<release>@sha256:<index-digest>` reference in:
   - `docker-compose.yml`
   - `test/integration/docker-compose.yml`
   - `test/simulation/docker-compose.yml`
4. Test the image from a clean local cache:

   ```bash
   docker image rm pgsty/silo:<old-release> || true
   docker compose -f test/integration/docker-compose.yml pull minio createbucket
   docker compose -f test/integration/docker-compose.yml up -d postgres minio --wait
   docker compose -f test/integration/docker-compose.yml up createbucket
   ./scripts/test-integration.sh
   ```

5. Let the pull request run on `linux/amd64`. When changing the image, also run the clean-cache Compose commands on `linux/arm64` (or inspect and pull that platform explicitly) before merging.

Digest pins do not receive fixes automatically. Check for new Silo releases during routine dependency maintenance and promptly after a Silo security advisory.
