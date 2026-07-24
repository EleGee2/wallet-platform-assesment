import { INestApplication } from '@nestjs/common';
import { Connection } from 'mongoose';
import { Wallet } from '../../src/wallets/schemas/wallet.schema';
import {
  createAuthenticatedRequest,
  createTestApp,
  flushThrottleState,
  getModel,
  resetDatabase,
} from './test-utils';

describe('Concurrent wallet operations (integration)', () => {
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

  it('never lets a wallet balance go negative under concurrent withdrawals', async () => {
    const wallet = await client
      .post('/wallets')
      .send({ userId: 'concurrent-1', ownerName: 'Akosua Darko' })
      .expect(201);

    await client.post(`/wallets/${wallet.body._id}/deposit`).send({ amount: 100 }).expect(201);

    const concurrentWithdrawals = 10;
    const results = await Promise.allSettled(
      Array.from({ length: concurrentWithdrawals }, () =>
        client.post(`/wallets/${wallet.body._id}/withdraw`).send({ amount: 20 }),
      ),
    );

    const finalWallet = await getModel(app, Wallet.name).findById(wallet.body._id);

    expect(finalWallet?.balance).toBeGreaterThanOrEqual(0);

    const successfulWithdrawals = results.filter(
      (result) => result.status === 'fulfilled' && (result.value as any).status === 201,
    ).length;
    expect(finalWallet?.balance).toBe(100 - successfulWithdrawals * 20);
  });
});
