# Site deployment

`streambed.dev` is built from this directory with Hugo Extended and published as a static site. Cloudflare Pages is the authoritative production host and CDN.

Commits to `main` deploy automatically to `https://streambed.dev`. Pull requests and non-production branches receive isolated preview deployments.

## Cloudflare Pages configuration

| Setting | Value |
|---|---|
| Production branch | `main` |
| Root directory | `site` |
| Build command | `./scripts/build-cloudflare.sh` |
| Build output directory | `public` |
| Production domains | `streambed.dev`, `www.streambed.dev` |

Production environment variables:

```text
HUGO_VERSION=0.146.0
GO_VERSION=1.22
HUGO_ENVIRONMENT=production
```

Preview environment variables:

```text
HUGO_VERSION=0.146.0
GO_VERSION=1.22
HUGO_ENVIRONMENT=preview
```

Do not override Hugo's configured `baseURL` with the Pages deployment URL. Canonical URLs must remain under `https://streambed.dev/`.

Cloudflare redirects `www.streambed.dev` to the apex while preserving paths and query strings. The production `streambed-site.pages.dev` hostname also redirects to the apex; branch and commit preview hostnames remain directly accessible.

The build script converts a shallow checkout to a full checkout before invoking Hugo. This is required because `hugo.yaml` enables Git metadata for page modification dates.

## Local validation

From this directory:

```bash
./scripts/build-cloudflare.sh
```

Before promoting a hosting change, verify the homepage, documentation, blog, sitemap, RSS feed, robots file, static assets, canonical URLs, modification dates, trailing-slash behavior, and 404 response.

Preview deployments must not emit the production Cloudflare Web Analytics beacon and should return `X-Robots-Tag: noindex`.
