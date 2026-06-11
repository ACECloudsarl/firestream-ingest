module.exports = {
  apps: [{
    name: 'firestream-dav',
    script: 'ingest.js',
    cwd: __dirname,
    autorestart: true,
    max_memory_restart: '1G',
    env: {
      NODE_ENV: 'production'
    },
    // Graceful shutdown — let in-flight PUTs drain
    kill_timeout: 15000,
    listen_timeout: 5000,
    // Log rotation
    log_date_format: 'YYYY-MM-DD HH:mm:ss',
    error_file: '/var/log/firestream-dav/error.log',
    out_file: '/var/log/firestream-dav/out.log',
    merge_logs: true
  }]
}
