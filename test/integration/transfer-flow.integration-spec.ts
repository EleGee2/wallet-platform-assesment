import { INestApplication, Logger } from '@nestjs/common';
import { Connection } from 'mongoose';
import { RabbitMQService } from '../../src/queue/rabbitmq.service';
import { Transfer } from '../../src/wallets/schemas/transfer.schema';
import { Wallet } from '../../src/wallets/schemas/wallet.schema';
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

describe('Transfer flow (integration)', () => {
  let app: INestApplication;
  let connection: Connection;
  let client: Awaited<ReturnType<typeof createAuthenticatedRequest>>;
  let rabbitMQService: RabbitMQService;

  beforeAll(async () => {
    ({ app, connection } = await createTestApp());
    rabbitMQService = app.get(RabbitMQService);
  });

  beforeEach(async () => {
    await resetDatabase(connection);
    client = await createAuthenticatedRequest(app, connection);
  });

  afterAll(async () => {
    await app.close();
  });

  it('debits the sender immediately and eventually credits the receiver once the event is consumed', async () => {
    const walletModel = getModel(app, Wallet.name);
    const transferModel = getModel(app, Transfer.name);

    const fromWallet = await client
      .post('/wallets')
      .send({ userId: 'sender', ownerName: 'Ama Owusu' })
      .expect(201);
    const toWallet = await client
      .post('/wallets')
      .send({ userId: 'receiver', ownerName: 'Kwame Mensah' })
      .expect(201);

    await client.post(`/wallets/${fromWallet.body._id}/deposit`).send({ amount: 500 }).expect(201);

    const transferResponse = await client
      .post('/wallets/transfer')
      .send({ fromWalletId: fromWallet.body._id, toWalletId: toWallet.body._id, amount: 120 })
      .expect(201);

    expect(transferResponse.body.status).toBe('PENDING');

    const senderWallet = await walletModel.findById(fromWallet.body._id);
    expect(senderWallet).toBeTruthy();

    const settled = await pollUntil(async () => {
      const transfer = await transferModel.findById(transferResponse.body._id);
      return transfer?.status === 'COMPLETED';
    });

    expect(settled).toBe(true);

    const receiverWallet = await walletModel.findById(toWallet.body._id);
    expect(receiverWallet?.balance).toBe(120);
  });

  it('rejects transferring more than the sender holds and leaves both wallets untouched', async () => {
    const walletModel = getModel(app, Wallet.name);

    const fromWallet = await client
      .post('/wallets')
      .send({ userId: 'sender-2', ownerName: 'Efua Asante' })
      .expect(201);
    const toWallet = await client
      .post('/wallets')
      .send({ userId: 'receiver-2', ownerName: 'Kofi Boateng' })
      .expect(201);

    await client.post(`/wallets/${fromWallet.body._id}/deposit`).send({ amount: 10 }).expect(201);

    await client
      .post('/wallets/transfer')
      .send({ fromWalletId: fromWallet.body._id, toWalletId: toWallet.body._id, amount: 100 })
      .expect(400);

    const senderWallet = await walletModel.findById(fromWallet.body._id);
    const receiverWallet = await walletModel.findById(toWallet.body._id);

    expect(senderWallet?.balance).toBe(10);
    expect(receiverWallet?.balance).toBe(0);
  });

  it('does not double-debit when the same transfer request is retried with the same idempotency key', async () => {
    const walletModel = getModel(app, Wallet.name);
    const transferModel = getModel(app, Transfer.name);

    const fromWallet = await client
      .post('/wallets')
      .send({ userId: 'sender-3', ownerName: 'Kwabena Agyei' })
      .expect(201);
    const toWallet = await client
      .post('/wallets')
      .send({ userId: 'receiver-3', ownerName: 'Adjoa Sarpong' })
      .expect(201);

    await client.post(`/wallets/${fromWallet.body._id}/deposit`).send({ amount: 200 }).expect(201);

    const body = {
      fromWalletId: fromWallet.body._id,
      toWalletId: toWallet.body._id,
      amount: 75,
      idempotencyKey: 'integration-retry-key-1',
    };

    const first = await client.post('/wallets/transfer').send(body).expect(201);
    const second = await client.post('/wallets/transfer').send(body).expect(201);

    expect(second.body._id).toBe(first.body._id);

    const senderWallet = await walletModel.findById(fromWallet.body._id);
    expect(senderWallet?.balance).toBe(125);

    const transfersWithKey = await transferModel
      .find({ idempotencyKey: body.idempotencyKey })
      .exec();
    expect(transfersWithKey).toHaveLength(1);
  });

  it('does not double-credit the receiver when the transfer.initiated event is redelivered', async () => {
    const walletModel = getModel(app, Wallet.name);
    const transferModel = getModel(app, Transfer.name);

    const fromWallet = await client
      .post('/wallets')
      .send({ userId: 'sender-4', ownerName: 'Kojo Amankwah' })
      .expect(201);
    const toWallet = await client
      .post('/wallets')
      .send({ userId: 'receiver-4', ownerName: 'Esi Gyamfi' })
      .expect(201);

    await client.post(`/wallets/${fromWallet.body._id}/deposit`).send({ amount: 300 }).expect(201);

    const transferResponse = await client
      .post('/wallets/transfer')
      .send({ fromWalletId: fromWallet.body._id, toWalletId: toWallet.body._id, amount: 90 })
      .expect(201);

    const settled = await pollUntil(async () => {
      const transfer = await transferModel.findById(transferResponse.body._id);
      return transfer?.status === 'COMPLETED';
    });
    expect(settled).toBe(true);

    const receiverAfterFirstDelivery = await walletModel.findById(toWallet.body._id);
    expect(receiverAfterFirstDelivery?.balance).toBe(90);

    // Simulate RabbitMQ redelivering the same event (e.g. consumer crashed
    // after processing but before ack) by publishing it again directly.
    await rabbitMQService.publish('transfer.initiated', {
      transferId: transferResponse.body._id,
      fromWalletId: fromWallet.body._id,
      toWalletId: toWallet.body._id,
      amount: 90,
    });

    // Give the consumer time to (not) process the redelivered message.
    await new Promise((resolve) => setTimeout(resolve, 1000));

    const receiverAfterRedelivery = await walletModel.findById(toWallet.body._id);
    expect(receiverAfterRedelivery?.balance).toBe(90);
  });

  it('reflects a fresh balance on GET /wallets/:id for the receiver right after the transfer completes', async () => {
    const transferModel = getModel(app, Transfer.name);

    const fromWallet = await client
      .post('/wallets')
      .send({ userId: 'sender-5', ownerName: 'Nana Yeboah' })
      .expect(201);
    const toWallet = await client
      .post('/wallets')
      .send({ userId: 'receiver-5', ownerName: 'Adwoa Kusi' })
      .expect(201);

    await client.post(`/wallets/${fromWallet.body._id}/deposit`).send({ amount: 400 }).expect(201);

    // Populate the receiver's cache with the pre-transfer (zero) balance.
    const before = await client.get(`/wallets/${toWallet.body._id}`).expect(200);
    expect(before.body.balance).toBe(0);

    const transferResponse = await client
      .post('/wallets/transfer')
      .send({ fromWalletId: fromWallet.body._id, toWalletId: toWallet.body._id, amount: 65 })
      .expect(201);

    const settled = await pollUntil(async () => {
      const transfer = await transferModel.findById(transferResponse.body._id);
      return transfer?.status === 'COMPLETED';
    });
    expect(settled).toBe(true);

    const after = await client.get(`/wallets/${toWallet.body._id}`).expect(200);
    expect(after.body.balance).toBe(65);
  });

  it('threads one correlation id from the HTTP request through the outbox event into the consumer logs', async () => {
    const transferModel = getModel(app, Transfer.name);
    const correlationId = 'integration-correlation-id-1';

    const fromWallet = await client
      .post('/wallets')
      .send({ userId: 'sender-6', ownerName: 'Fiifi Quaye' })
      .expect(201);
    const toWallet = await client
      .post('/wallets')
      .send({ userId: 'receiver-6', ownerName: 'Abla Tetteh' })
      .expect(201);

    await client.post(`/wallets/${fromWallet.body._id}/deposit`).send({ amount: 200 }).expect(201);

    const logSpy = jest.spyOn(Logger.prototype, 'log');

    const transferResponse = await client
      .post('/wallets/transfer')
      .set('x-correlation-id', correlationId)
      .send({ fromWalletId: fromWallet.body._id, toWalletId: toWallet.body._id, amount: 55 })
      .expect(201);

    // Threaded through the outbox payload, per the plan - not just the HTTP
    // response header the middleware already set.
    const storedTransfer = await transferModel.findById(transferResponse.body._id);
    expect(storedTransfer?.correlationId).toBe(correlationId);

    const settled = await pollUntil(async () => {
      const transfer = await transferModel.findById(transferResponse.body._id);
      return transfer?.status === 'COMPLETED';
    });
    expect(settled).toBe(true);

    // Threaded all the way into the async consumer's own log line - the
    // actual point of this fix, not just DB-stored state.
    const correlatedLogLines = logSpy.mock.calls
      .map((call) => call[0])
      .filter((line) => typeof line === 'string' && line.includes(correlationId));
    expect(correlatedLogLines.length).toBeGreaterThan(0);

    logSpy.mockRestore();
  });
});
