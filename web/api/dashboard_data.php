<?php

declare(strict_types=1);

const DEFAULT_DAYS = 90;
const QUERY_LIMIT = 5000;

header('Content-Type: application/json; charset=utf-8');
header('Cache-Control: no-store');
header('X-Content-Type-Options: nosniff');
header('X-Frame-Options: SAMEORIGIN');
header('Referrer-Policy: no-referrer');

if (($_SERVER['REQUEST_METHOD'] ?? 'GET') !== 'GET') {
    http_response_code(405);
    echo json_encode(['ok' => false, 'error' => 'Method not allowed'], JSON_UNESCAPED_SLASHES);
    exit;
}

// ── Rate limiting: 60 requests/minute per IP ──
(function () {
    $ip = $_SERVER['REMOTE_ADDR'] ?? '0.0.0.0';
    $hash = substr(md5($ip), 0, 12);
    $rateFile = sys_get_temp_dir() . '/dashboard_rate_' . $hash;
    $limit = 60;
    $window = 60; // seconds

    $now = time();
    $timestamps = [];
    if (is_file($rateFile)) {
        $raw = @file_get_contents($rateFile);
        if ($raw !== false) {
            $timestamps = array_filter(
                array_map('intval', explode("\n", trim($raw))),
                fn(int $ts) => ($now - $ts) < $window
            );
        }
    }
    if (count($timestamps) >= $limit) {
        http_response_code(429);
        header('Retry-After: ' . $window);
        echo json_encode(['ok' => false, 'error' => 'Rate limit exceeded'], JSON_UNESCAPED_SLASHES);
        exit;
    }
    $timestamps[] = $now;
    @file_put_contents($rateFile, implode("\n", $timestamps));
})();

function respond(array $payload, int $statusCode = 200): void
{
    http_response_code($statusCode);
    echo json_encode($payload, JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE);
    exit;
}

function fail(string $message, int $statusCode = 500, ?string $details = null): void
{
    if ($details) {
        error_log('[dashboard_data] ' . $message . ' — ' . $details);
    }
    respond(['ok' => false, 'error' => $message], $statusCode);
}

function loadRuntimeConfig(): array
{
    $config = [
        'supabase_url' => (string) (getenv('SUPABASE_URL') ?: ''),
        'supabase_service_role_key' => (string) (getenv('SUPABASE_SERVICE_ROLE_KEY') ?: ''),
    ];

    $localConfigPath = __DIR__ . '/config.local.php';
    if (is_file($localConfigPath)) {
        $localConfig = require $localConfigPath;
        if (is_array($localConfig)) {
            if (!empty($localConfig['supabase_url'])) {
                $config['supabase_url'] = (string) $localConfig['supabase_url'];
            }
            if (!empty($localConfig['supabase_service_role_key'])) {
                $config['supabase_service_role_key'] = (string) $localConfig['supabase_service_role_key'];
            }
        }
    }

    $config['supabase_url'] = rtrim($config['supabase_url'], '/');
    // Strip any accidental whitespace/newlines from key (common when pasting in editors)
    $config['supabase_service_role_key'] = preg_replace('/\s+/', '', $config['supabase_service_role_key']);
    if ($config['supabase_url'] === '' || $config['supabase_service_role_key'] === '') {
        fail('Missing API server config. Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY on server.', 500);
    }

    return $config;
}

function buildQueryString(array $pairs): string
{
    $chunks = [];
    foreach ($pairs as $pair) {
        if (!is_array($pair) || count($pair) !== 2) {
            continue;
        }
        $chunks[] = rawurlencode((string) $pair[0]) . '=' . rawurlencode((string) $pair[1]);
    }
    return implode('&', $chunks);
}

function httpGet(string $url, array $headers): array
{
    if (function_exists('curl_init')) {
        $ch = curl_init($url);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($ch, CURLOPT_TIMEOUT, 20);
        curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
        $body = curl_exec($ch);
        $status = (int) curl_getinfo($ch, CURLINFO_HTTP_CODE);
        if ($body === false) {
            $err = curl_error($ch);
            curl_close($ch);
            fail('Upstream request failed.', 502, $err ?: 'Unknown cURL error');
        }
        curl_close($ch);
        return [$status, (string) $body];
    }

    $context = stream_context_create([
        'http' => [
            'method' => 'GET',
            'header' => implode("\r\n", $headers),
            'timeout' => 20,
            'ignore_errors' => true,
        ],
    ]);

    $body = @file_get_contents($url, false, $context);
    if ($body === false) {
        fail('Upstream request failed.', 502, 'Failed to read response stream');
    }

    $status = 200;
    $httpResponseHeader = $http_response_header ?? [];
    foreach ($httpResponseHeader as $line) {
        if (preg_match('/\s(\d{3})\s/', $line, $matches) === 1) {
            $status = (int) $matches[1];
            break;
        }
    }

    return [$status, (string) $body];
}

function supabaseGet(string $baseUrl, string $serviceRoleKey, string $table, array $queryPairs): array
{
    $query = buildQueryString($queryPairs);
    $url = sprintf('%s/rest/v1/%s?%s', $baseUrl, rawurlencode($table), $query);
    [$status, $body] = httpGet(
        $url,
        [
            'apikey: ' . $serviceRoleKey,
            'Authorization: Bearer ' . $serviceRoleKey,
            'Accept: application/json',
            'Accept-Profile: public',
        ]
    );

    $decoded = json_decode($body, true);
    if ($status >= 400) {
        if (is_array($decoded) && !empty($decoded['message'])) {
            fail('Supabase query failed.', 502, (string) $decoded['message']);
        }
        fail('Supabase query failed.', 502);
    }

    if (!is_array($decoded)) {
        fail('Supabase returned invalid JSON.', 502);
    }

    return $decoded;
}

function parseIsoDate(string $isoDate): ?DateTimeImmutable
{
    $dt = DateTimeImmutable::createFromFormat('!Y-m-d', $isoDate, new DateTimeZone('UTC'));
    return $dt ?: null;
}

function readDays(): int
{
    $raw = $_GET['days'] ?? DEFAULT_DAYS;
    $days = (int) $raw;
    if ($days < 1 || $days > 365) {
        fail('Invalid days query parameter. Use a value between 1 and 365.', 400);
    }
    return $days;
}

$config = loadRuntimeConfig();
$days = readDays();

$metadataRows = supabaseGet(
    $config['supabase_url'],
    $config['supabase_service_role_key'],
    'usage_dashboard_metadata',
    [
        ['select', '*'],
        ['metadata_key', 'eq.global'],
        ['limit', '1'],
    ]
);

$metadata = $metadataRows[0] ?? [];
$metadataRangeEnd = isset($metadata['range_end']) ? (string) $metadata['range_end'] : '';
$rangeEnd = parseIsoDate($metadataRangeEnd);
if (!$rangeEnd) {
    $rangeEnd = new DateTimeImmutable('today', new DateTimeZone('UTC'));
}
$rangeStart = $rangeEnd->sub(new DateInterval('P' . max($days - 1, 0) . 'D'));
$rangeStartIso = $rangeStart->format('Y-m-d');
$rangeEndIso = $rangeEnd->format('Y-m-d');

$sourceRows = supabaseGet(
    $config['supabase_url'],
    $config['supabase_service_role_key'],
    'usage_sources_daily',
    [
        ['select', 'usage_date,source,cost_usd,total_tokens,sessions,tool_calls,mcp_calls,events,cache_read_tokens,cache_write_tokens,reasoning_tokens,subagent_count,thinking_blocks,avg_turn_duration_ms,rate_limit_max_pct,rate_limit_daily_max_pct'],
        ['usage_date', 'gte.' . $rangeStartIso],
        ['usage_date', 'lte.' . $rangeEndIso],
        ['order', 'usage_date.asc'],
        ['limit', (string) QUERY_LIMIT],
    ]
);

$projectRows = supabaseGet(
    $config['supabase_url'],
    $config['supabase_service_role_key'],
    'usage_projects_daily',
    [
        ['select', 'usage_date,project_name,project_hash,source,cost_usd,total_tokens,sessions,tool_calls,mcp_calls'],
        ['usage_date', 'gte.' . $rangeStartIso],
        ['usage_date', 'lte.' . $rangeEndIso],
        ['order', 'cost_usd.desc'],
        ['limit', (string) QUERY_LIMIT],
    ]
);

$modelRows = supabaseGet(
    $config['supabase_url'],
    $config['supabase_service_role_key'],
    'usage_models_daily',
    [
        ['select', 'usage_date,model,source,total_tokens,cost_usd,events'],
        ['usage_date', 'gte.' . $rangeStartIso],
        ['usage_date', 'lte.' . $rangeEndIso],
        ['order', 'total_tokens.desc'],
        ['limit', (string) QUERY_LIMIT],
    ]
);

$sessionRows = supabaseGet(
    $config['supabase_url'],
    $config['supabase_service_role_key'],
    'usage_sessions_daily',
    [
        ['select', 'usage_date,source,session_id,events,tool_calls,mcp_calls,failed_tool_calls,user_messages,assistant_messages,back_forth_pairs,code_lines_written,files_touched_count,cache_read_tokens,cache_write_tokens,reasoning_tokens,subagent_count,thinking_blocks,avg_turn_duration_ms,total_turn_duration_ms,rate_limit_max_pct,rate_limit_daily_max_pct'],
        ['usage_date', 'gte.' . $rangeStartIso],
        ['usage_date', 'lte.' . $rangeEndIso],
        ['order', 'usage_date.asc'],
        ['limit', (string) QUERY_LIMIT],
    ]
);

$toolCallRows = supabaseGet(
    $config['supabase_url'],
    $config['supabase_service_role_key'],
    'usage_tool_calls',
    [
        ['select', 'tool_name,is_mcp,success'],
        ['usage_date', 'gte.' . $rangeStartIso],
        ['usage_date', 'lte.' . $rangeEndIso],
        ['limit', '10000'],
    ]
);

respond([
    'ok' => true,
    'days' => $days,
    'range_start' => $rangeStartIso,
    'range_end' => $rangeEndIso,
    'metadata' => $metadata,
    'source_rows' => $sourceRows,
    'project_rows' => $projectRows,
    'model_rows' => $modelRows,
    'session_rows' => $sessionRows,
    'tool_call_rows' => $toolCallRows,
]);
