import { INestApplication } from '@nestjs/common';
import { Connection } from 'mongoose';
import {
  createAuthenticatedRequest,
  createTestApp,
  flushThrottleState,
  resetDatabase,
} from './test-utils';

describe('Transactions (integration)', () => {
  let app: INestApplication;
  let connection: Connection;
  let client: Awaited<ReturnType<typeof createAuthenticatedRequest>>;

  beforeAll(async () => {
    ({ app, connection } = await createTestApp());
  });

  beforeEach(async () => {
    await resetDatabase(connection);
    await flushThrottleState(app);
    client = await createAuthenticatedRequest(app, connection);
  });

  afterAll(async () => {
    await app.close();
  });

  it('filters by type alone, with no walletId, across every wallet', async () => {
    const walletA = await client
      .post('/wallets')
      .send({ userId: 'txn-filter-a', ownerName: 'Yaa Asantewaa' })
      .expect(201);
    const walletB = await client
      .post('/wallets')
      .send({ userId: 'txn-filter-b', ownerName: 'Kojo Antwi' })
      .expect(201);

    await client.post(`/wallets/${walletA.body._id}/deposit`).send({ amount: 200 }).expect(201);
    await client.post(`/wallets/${walletA.body._id}/withdraw`).send({ amount: 50 }).expect(201);
    await client.post(`/wallets/${walletB.body._id}/deposit`).send({ amount: 300 }).expect(201);
    await client.post(`/wallets/${walletB.body._id}/withdraw`).send({ amount: 75 }).expect(201);

    const withdrawals = await client.get('/transactions?type=WITHDRAWAL').expect(200);

    expect(withdrawals.body.total).toBe(2);
    expect(withdrawals.body.items.every((item: any) => item.type === 'WITHDRAWAL')).toBe(true);
    // Spans both wallets - proves this isn't silently scoped to one.
    const walletIds = withdrawals.body.items.map((item: any) => item.walletId);
    expect(new Set(walletIds).size).toBe(2);
  });

  it('builds the type index declared on the schema', async () => {
    // Mongoose builds declared indexes in the background on connect - a
    // short delay is the only way to observe this against a real
    // collection, since a mocked unit test can't see an index at all.
    await new Promise((resolve) => setTimeout(resolve, 500));

    const indexes = await connection.db!.collection('transactions').indexes();
    const hasTypeIndex = indexes.some(
      (index) => index.key.type === 1 && index.key.createdAt === -1,
    );

    expect(hasTypeIndex).toBe(true);
  });
});
