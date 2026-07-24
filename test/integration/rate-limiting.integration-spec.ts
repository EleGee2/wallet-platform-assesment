import { INestApplication } from '@nestjs/common';
import { Connection } from 'mongoose';
import {
  createAuthenticatedRequest,
  createTestApp,
  flushThrottleState,
  resetDatabase,
} from './test-utils';

describe('Rate limiting (integration)', () => {
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

  it('throttles POST /wallets/transfer once its tighter override limit is exceeded', async () => {
    const fromWallet = await client
      .post('/wallets')
      .send({ userId: 'rate-limit-sender', ownerName: 'Naa Adjeley' })
      .expect(201);
    const toWallet = await client
      .post('/wallets')
      .send({ userId: 'rate-limit-receiver', ownerName: 'Nii Armah' })
      .expect(201);

    await client
      .post(`/wallets/${fromWallet.body._id}/deposit`)
      .send({ amount: 10_000 })
      .expect(201);

    // Override is 15 requests per 10s - fire past it. Sequential, not
    // concurrent: rate limiting is a counting concern, not a race - the
    // Redis Lua script already guarantees the counter itself is atomic
    // under real concurrency, so a request-per-request loop here proves the
    // threshold behavior without the added complexity of a concurrent burst.
    const statuses: number[] = [];
    for (let i = 0; i < 20; i += 1) {
      const response = await client.post('/wallets/transfer').send({
        fromWalletId: fromWallet.body._id,
        toWalletId: toWallet.body._id,
        amount: 1,
      });
      statuses.push(response.status);
    }

    expect(statuses).toContain(429);
    expect(statuses.filter((status) => status !== 429).length).toBeLessThanOrEqual(15);
  });

  it('does not throttle an unrelated route within the same window', async () => {
    const wallet = await client
      .post('/wallets')
      .send({ userId: 'rate-limit-unaffected', ownerName: 'Ama Darko' })
      .expect(201);

    // Well past the transfer override's limit (15), well under the global
    // default (100) - proves the tighter override is scoped to its own
    // route, not applied globally.
    for (let i = 0; i < 20; i += 1) {
      await client.get(`/wallets/${wallet.body._id}`).expect(200);
    }
  });
});
