import { INestApplication } from '@nestjs/common';
import { Connection, Types } from 'mongoose';
import { RabbitMQService } from '../../src/queue/rabbitmq.service';
import { createTestApp, flushThrottleState, resetDatabase } from './test-utils';

async function pollUntil(fn: () => Promise<boolean>, timeoutMs = 8000, intervalMs = 200) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await fn()) {
      return true;
    }
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
  return false;
}

describe('RabbitMQ publishing (integration)', () => {
  let app: INestApplication;
  let connection: Connection;
  let rabbitMQService: RabbitMQService;

  beforeAll(async () => {
    ({ app, connection } = await createTestApp());
    rabbitMQService = app.get(RabbitMQService);
  });

  beforeEach(async () => {
    await resetDatabase(connection);
    await flushThrottleState(app);
  });

  afterAll(async () => {
    await app.close();
  });

  it('publishes without throwing against a real broker connection', async () => {
    await expect(
      rabbitMQService.publish('transfer.initiated', {
        transferId: 'test-transfer',
        fromWalletId: 'a',
        toWalletId: 'b',
        amount: 10,
      }),
    ).resolves.not.toThrow();
  });

  it('dead-letters a transfer event that fails processing twice in a row', async () => {
    const transferId = new Types.ObjectId();
    await connection.collection('transfers').insertOne({
      _id: transferId,
      fromWalletId: new Types.ObjectId(),
      toWalletId: new Types.ObjectId(),
      amount: 10,
      status: 'PENDING',
      createdAt: new Date(),
      updatedAt: new Date(),
    });

    // toWalletId doesn't correspond to any real wallet, so completeTransfer's
    // wallet lookup throws on every delivery attempt - the transfer's status
    // guard rolls back with it (same transaction), so redelivery reliably
    // reproduces the same failure instead of silently no-op'ing.
    await rabbitMQService.publish('transfer.initiated', {
      transferId: transferId.toString(),
      fromWalletId: new Types.ObjectId().toString(),
      toWalletId: new Types.ObjectId().toString(),
      amount: 10,
    });

    // Poll by content, not just queue depth: the previous test's own
    // 'test-transfer' message independently fails and dead-letters on its own
    // schedule, so a bare message-count check can pass on the wrong message
    // while this one is still mid-retry - draining unrelated messages here
    // instead of ignoring them keeps the queue clean for a re-run too.
    const channelWrapper = rabbitMQService.getChannelWrapper();
    let foundOwnMessage = false;
    const deadLettered = await pollUntil(async () => {
      const message = await channelWrapper.get(rabbitMQService.getDeadLetterQueue());
      if (!message) {
        return false;
      }
      channelWrapper.ack(message);
      const payload = JSON.parse(message.content.toString());
      foundOwnMessage = foundOwnMessage || payload.transferId === transferId.toString();
      return foundOwnMessage;
    });

    expect(deadLettered).toBe(true);
  });
});
