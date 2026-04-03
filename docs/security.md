# Security Notes

## Safe to Commit

- `README.md`
- `docs/*`
- `supabase/schema_usage.sql`
- `supabase/migrations/*`
- `web/index.html`
- `web/app.js`
- `web/styles.css`
- `web/config.example.js`
- `web/api/dashboard_data.php`
- `web/api/views.php`
- `web/api/config.local.example.php`

## Never Commit

- `.env`
- `web/api/config.local.php`
- any real access token, password, or private key
- any raw personal logs or generated datasets from your own account

## Secret Boundaries

### Browser-safe files

- `web/index.html`
- `web/app.js`
- `web/styles.css`
- `web/config.js`

These must not contain:

- `SUPABASE_SERVICE_ROLE_KEY`
- JWT-like bearer tokens
- DB passwords
- private keys

### Server-only files

- `web/api/config.local.php`
- host environment variables

These may contain:

- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`

## Public Repo Checklist

Before publishing this repo:

1. Confirm all secrets are placeholders only.
2. Confirm no personal paths remain, such as `/Users/<real-name>/...`.
3. Confirm sample payloads contain fake data only.
4. Confirm no deployment-only local files were copied into git.

## Post-Deploy Checks

```bash
curl -sL https://YOUR_DOMAIN/path/config.js
curl -sL https://YOUR_DOMAIN/path/app.js | rg -n "SUPABASE|BEGIN .*PRIVATE KEY|eyJ"
curl -sL "https://YOUR_DOMAIN/path/api/dashboard_data.php?days=30"
curl -I -L https://YOUR_DOMAIN/path/api/config.local.php
```

Expected:

- browser JS contains no secrets
- `dashboard_data.php` returns JSON
- `config.local.php` is not directly downloadable
- `config.js` contains only browser-safe values such as API endpoint and default range
