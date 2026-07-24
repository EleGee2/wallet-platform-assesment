export default () => ({
  port: parseInt(process.env.PORT || '3000', 10),

  mongodb: {
    uri:
      process.env.MONGODB_URI ||
      'mongodb://localhost:27017/wallet-platform?replicaSet=rs0&directConnection=true',
  },

  redis: {
    host: process.env.REDIS_HOST || 'localhost',
    port: parseInt(process.env.REDIS_PORT || '6379', 10),
    ttlSeconds: parseInt(process.env.REDIS_TTL_SECONDS || '3600', 10),
  },

  rabbitmq: {
    uri: process.env.RABBITMQ_URI || 'amqp://guest:guest@localhost:5672',
    exchange: process.env.RABBITMQ_EXCHANGE || 'wallet.events',
    transferQueue: process.env.RABBITMQ_TRANSFER_QUEUE || 'transfer.events.queue',
    deadLetterExchange: process.env.RABBITMQ_DEAD_LETTER_EXCHANGE || 'wallet.events.dlx',
    deadLetterQueue: process.env.RABBITMQ_DEAD_LETTER_QUEUE || 'transfer.events.dlq',
  },

  auth: {
    jwtSecret: process.env.JWT_SECRET || 'change-me-in-production',
    jwtExpiresIn: process.env.JWT_EXPIRES_IN || '3600s',
  },

  rateLimit: {
    ttlMs: parseInt(process.env.RATE_LIMIT_TTL_MS || '60000', 10),
    limit: parseInt(process.env.RATE_LIMIT_LIMIT || '100', 10),
  },

  workers: {
    outboxRelayIntervalMs: parseInt(process.env.OUTBOX_RELAY_INTERVAL_MS || '2000', 10),
    pendingTransferSweepIntervalMs: parseInt(
      process.env.PENDING_TRANSFER_SWEEP_INTERVAL_MS || '5000',
      10,
    ),
    pendingTransferTimeoutMs: parseInt(process.env.PENDING_TRANSFER_TIMEOUT_MS || '60000', 10),
  },
});
