import { INestApplication } from '@nestjs/common';
import { Connection, Types } from 'mongoose';
import { RabbitMQService } from '../../src/queue/rabbitmq.service';
import { Transfer } from '../../src/wallets/schemas/transfer.schema';
import { Wallet } from '../../src/wallets/schemas/wallet.schema';
import { PendingTransferWorker } from '../../src/workers/pending-transfer.worker';
import { createAuthenticatedRequest, createTestApp, getModel, resetDatabase } from './test-utils';

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

describe('PendingTransferWorker sweep (integration)', () => {
  let app: INestApplication;
  let connection: Connection;
  let client: Awaited<ReturnType<typeof createAuthenticatedRequest>>;
  let worker: PendingTransferWorker;
  let rabbitMQService: RabbitMQService;

  beforeAll(async () => {
    ({ app, connection } = await createTestApp());
    worker = app.get(PendingTransferWorker);
    rabbitMQService = app.get(RabbitMQService);
    // The worker's own interval would otherwise race with the manual sweep()
    // calls below and inflate the publish-call assertions non-deterministically.
    clearInterval((worker as any).timer);
  });

  beforeEach(async () => {
    await resetDatabase(connection);
    client = await createAuthenticatedRequest(app, connection);
  });

  afterAll(async () => {
    await app.close();
  });

  it('re-publishes a stale transfer at most once per timeout window, and it still completes normally', async () => {
    const walletModel = getModel(app, Wallet.name);
    const transferModel = getModel(app, Transfer.name);
    const publishSpy = jest.spyOn(rabbitMQService, 'publish');

    const fromWallet = await client
      .post('/wallets')
      .send({ userId: 'sweep-sender', ownerName: 'Yaw Appiah' })
      .expect(201);
    const toWallet = await client
      .post('/wallets')
      .send({ userId: 'sweep-receiver', ownerName: 'Abena Osei' })
      .expect(201);

    await client.post(`/wallets/${fromWallet.body._id}/deposit`).send({ amount: 400 }).expect(201);

    // Inserted directly (not via POST /wallets/transfer) so no outbox event was
    // ever published for it - the only way it gets processed is via sweep().
    const transferId = new Types.ObjectId();
    await connection.collection('transfers').insertOne({
      _id: transferId,
      fromWalletId: new Types.ObjectId(fromWallet.body._id),
      toWalletId: new Types.ObjectId(toWallet.body._id),
      amount: 60,
      status: 'PENDING',
      createdAt: new Date(Date.now() - 61_000),
      updatedAt: new Date(Date.now() - 61_000),
    });

    await (worker as any).sweep();
    expect(publishSpy).toHaveBeenCalledTimes(1);

    // Immediately swept again, before the consumer has necessarily finished
    // processing the first publish - lastSweptAt (just set) must exclude it.
    await (worker as any).sweep();
    expect(publishSpy).toHaveBeenCalledTimes(1);

    const settled = await pollUntil(async () => {
      const transfer = await transferModel.findById(transferId);
      return transfer?.status === 'COMPLETED';
    });
    expect(settled).toBe(true);

    const receiverWallet = await walletModel.findById(toWallet.body._id);
    expect(receiverWallet?.balance).toBe(60);
  });
});
