<?php
header('Content-Type: application/json');
header('Access-Control-Allow-Origin: *');
header('Cache-Control: no-store');

// File-based atomic counter — stored next to this script on the server
$file = __DIR__ . '/views.count';

// Read current count
$count = 0;
if (file_exists($file)) {
    $count = (int) trim(file_get_contents($file));
}

// Only increment on real page loads (not prefetch/bot crawlers)
$ua = $_SERVER['HTTP_USER_AGENT'] ?? '';
$is_prefetch = isset($_SERVER['HTTP_PURPOSE']) && $_SERVER['HTTP_PURPOSE'] === 'prefetch';
$is_bot = preg_match('/bot|crawl|spider|slurp|mediapartners/i', $ua);

if (!$is_prefetch && !$is_bot) {
    $count++;
    // Atomic write via temp file rename
    $tmp = $file . '.tmp.' . getmypid();
    file_put_contents($tmp, $count);
    rename($tmp, $file);
}

echo json_encode(['ok' => true, 'count' => $count]);
