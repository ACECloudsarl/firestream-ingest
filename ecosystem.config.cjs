module.exports = {
  apps: [
    {
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
    },
    {
      name: 'firestream-transfers',
      script: 'transfers.js',
      cwd: __dirname,
      autorestart: true,
      max_memory_restart: '512M',
      env: {
        NODE_ENV: 'production'
      },
      // Graceful shutdown — let in-flight downloads drain (max 5 min)
      kill_timeout: 300000,
      log_date_format: 'YYYY-MM-DD HH:mm:ss',
      error_file: '/var/log/firestream-dav/transfers-error.log',
      out_file: '/var/log/firestream-dav/transfers-out.log',
      merge_logs: true
    },
    {
      name: 'firestream-storage',
      script: 'scripts/update-storage.js',
      cwd: __dirname,
      autorestart: false,
      // Run every 60 seconds
      cron_restart: '* * * * *',
      restart_delay: 5000,
      env: {
        NODE_ENV: 'production'
      },
      log_date_format: 'YYYY-MM-DD HH:mm:ss',
      error_file: '/var/log/firestream-dav/storage-error.log',
      out_file: '/var/log/firestream-dav/storage-out.log',
      merge_logs: true
    },
    {
      name: 'firestream-cleanup',
      script: 'scripts/cleanup-videos.js',
      args: '--execute --batch-size 25 --scheduled',
      cwd: __dirname,
      autorestart: false,
      cron_restart: '0 0,12 * * *',
      max_memory_restart: '512M',
      env: {
        NODE_ENV: 'production',
        TZ: 'Europe/Berlin'
      },
      log_date_format: 'YYYY-MM-DD HH:mm:ss',
      error_file: '/var/log/firestream-dav/cleanup-error.log',
      out_file: '/var/log/firestream-dav/cleanup-out.log',
      merge_logs: true
    },
    {
      name: 'firestream-cold-transfer',
      script: 'scripts/cold-storage-transfer.js',
      args: '--execute',
      cwd: __dirname,
      autorestart: true,
      max_memory_restart: '1G',
      kill_timeout: 900000,
      env: {
        NODE_ENV: 'production'
      },
      log_date_format: 'HH:mm:ss',
      error_file: '/var/log/firestream-dav/cold-transfer-error.log',
      out_file: '/var/log/firestream-dav/cold-transfer-out.log',
      merge_logs: true
    },
    {
      name: 'firestream-cold-stage',
      script: 'scripts/cold-storage-stage.js',
      args: '--execute',
      cwd: __dirname,
      autorestart: true,
      max_memory_restart: '1G',
      kill_timeout: 900000,
      env: {
        NODE_ENV: 'production'
      },
      log_date_format: 'YYYY-MM-DD HH:mm:ss',
      error_file: '/var/log/firestream-dav/cold-stage-error.log',
      out_file: '/var/log/firestream-dav/cold-stage-out.log',
      merge_logs: true
    },
    {
      name: 'firestream-cold-restore',
      script: 'scripts/cold-storage-restore.js',
      cwd: __dirname,
      autorestart: true,
      max_memory_restart: '1G',
      kill_timeout: 900000,
      env: {
        NODE_ENV: 'production'
      },
      log_date_format: 'YYYY-MM-DD HH:mm:ss',
      error_file: '/var/log/firestream-dav/cold-restore-error.log',
      out_file: '/var/log/firestream-dav/cold-restore-out.log',
      merge_logs: true
    },
    {
      name: 'poster-upload',
      script: 'scripts/poster-upload-server.js',
      cwd: __dirname,
      autorestart: true,
      max_memory_restart: '256M',
      env: {
        NODE_ENV: 'production',
        POSTER_UPLOAD_PORT: '3001'
      },
      log_date_format: 'YYYY-MM-DD HH:mm:ss',
      error_file: '/var/log/firestream-dav/poster-upload-error.log',
      out_file: '/var/log/firestream-dav/poster-upload-out.log',
      merge_logs: true
    },
    {
      name: 'mini-poster',
      script: 'scripts/mini-poster-server.js',
      cwd: __dirname,
      autorestart: true,
      max_memory_restart: '512M',
      env: {
        NODE_ENV: 'production',
        MINI_POSTER_PORT: '3000'
      },
      log_date_format: 'YYYY-MM-DD HH:mm:ss',
      error_file: '/var/log/firestream-dav/mini-poster-error.log',
      out_file: '/var/log/firestream-dav/mini-poster-out.log',
      merge_logs: true
    },
    {
      name: 'subtitle-server',
      script: 'scripts/subtitle-server.js',
      cwd: __dirname,
      autorestart: true,
      max_memory_restart: '256M',
      env: {
        NODE_ENV: 'production',
        SUBTITLE_PORT: '3002'
      },
      log_date_format: 'YYYY-MM-DD HH:mm:ss',
      error_file: '/var/log/firestream-dav/subtitle-error.log',
      out_file: '/var/log/firestream-dav/subtitle-out.log',
      merge_logs: true
    }
  ]
}
