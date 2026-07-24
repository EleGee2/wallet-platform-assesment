import { INestApplication } from '@nestjs/common';
import { Connection } from 'mongoose';
import { LedgerEntry } from '../../src/ledger/schemas/ledger-entry.schema';
import { Transaction } from '../../src/transactions/schemas/transaction.schema';
import { Wallet } from '../../src/wallets/schemas/wallet.schema';
import {
  createAuthenticatedRequest,
  createTestApp,
  flushThrottleState,
  getModel,
  resetDatabase,
} from './test-utils';

describe('Wallets (integration)', () => {
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

  it('creates a wallet with a zero balance', async () => {
    const response = await client
      .post('/wallets')
      .send({ userId: 'user-1', ownerName: 'Ama Owusu' })
      .expect(201);

    expect(response.body.balance).toBe(0);
    expect(response.body.ownerName).toBe('Ama Owusu');
  });

  it('deposits funds and persists a matching ledger entry', async () => {
    const wallet = await client
      .post('/wallets')
      .send({ userId: 'user-2', ownerName: 'Kwame Mensah' })
      .expect(201);

    await client.post(`/wallets/${wallet.body._id}/deposit`).send({ amount: 200 }).expect(201);

    const ledgerEntryModel = getModel(app, LedgerEntry.name);
    const ledgerEntries = await ledgerEntryModel.find({ walletId: wallet.body._id }).exec();

    expect(ledgerEntries).toHaveLength(1);
    expect(ledgerEntries[0].direction).toBe('CREDIT');
    expect(ledgerEntries[0].amount).toBe(200);
  });

  it('rejects a withdrawal larger than the current balance', async () => {
    const wallet = await client
      .post('/wallets')
      .send({ userId: 'user-3', ownerName: 'Efua Asante' })
      .expect(201);

    await client.post(`/wallets/${wallet.body._id}/deposit`).send({ amount: 50 }).expect(201);

    await client.post(`/wallets/${wallet.body._id}/withdraw`).send({ amount: 100 }).expect(400);
  });

  it('rejects malformed wallet creation payloads', async () => {
    await client.post('/wallets').send({ ownerName: 'Missing userId' }).expect(400);
  });

  it('reflects a fresh balance on GET /wallets/:id immediately after a deposit', async () => {
    const wallet = await client
      .post('/wallets')
      .send({ userId: 'user-4', ownerName: 'Kwabena Agyei' })
      .expect(201);

    // Populate the cache with the pre-deposit (zero) balance.
    const before = await client.get(`/wallets/${wallet.body._id}`).expect(200);
    expect(before.body.balance).toBe(0);

    await client.post(`/wallets/${wallet.body._id}/deposit`).send({ amount: 150 }).expect(201);

    const after = await client.get(`/wallets/${wallet.body._id}`).expect(200);
    expect(after.body.balance).toBe(150);
  });

  it('does not double-credit when the same deposit request is retried with the same reference', async () => {
    const wallet = await client
      .post('/wallets')
      .send({ userId: 'user-5', ownerName: 'Adjoa Sarpong' })
      .expect(201);

    const body = { amount: 80, reference: 'integration-deposit-key-1' };

    await client.post(`/wallets/${wallet.body._id}/deposit`).send(body).expect(201);
    await client.post(`/wallets/${wallet.body._id}/deposit`).send(body).expect(409);

    const after = await client.get(`/wallets/${wallet.body._id}`).expect(200);
    expect(after.body.balance).toBe(80);

    const transactionModel = getModel(app, Transaction.name);
    const transactionsWithReference = await transactionModel
      .find({ reference: body.reference })
      .exec();
    expect(transactionsWithReference).toHaveLength(1);
  });

  it('computes correct dashboard totals/count and caps recentActivity at 10 for a wallet with more than 10 transactions', async () => {
    const wallet = await client
      .post('/wallets')
      .send({ userId: 'user-6', ownerName: 'Kojo Amankwah' })
      .expect(201);

    for (let i = 0; i < 12; i += 1) {
      await client.post(`/wallets/${wallet.body._id}/deposit`).send({ amount: 100 }).expect(201);
    }
    for (let i = 0; i < 3; i += 1) {
      await client.post(`/wallets/${wallet.body._id}/withdraw`).send({ amount: 50 }).expect(201);
    }

    const dashboard = await client.get(`/wallets/${wallet.body._id}/dashboard`).expect(200);

    expect(dashboard.body.transactionCount).toBe(15);
    expect(dashboard.body.totalDeposited).toBe(1200);
    expect(dashboard.body.totalWithdrawn).toBe(150);
    expect(dashboard.body.recentActivity).toHaveLength(10);
    for (const activity of dashboard.body.recentActivity) {
      expect(activity.transaction).toBeTruthy();
      expect(Array.isArray(activity.entries)).toBe(true);
      expect(activity.entries.length).toBeGreaterThan(0);
    }
  });

  it('reports a reconciled wallet after a mix of deposits and withdrawals', async () => {
    const wallet = await client
      .post('/wallets')
      .send({ userId: 'user-7', ownerName: 'Esi Gyamfi' })
      .expect(201);

    await client.post(`/wallets/${wallet.body._id}/deposit`).send({ amount: 300 }).expect(201);
    await client.post(`/wallets/${wallet.body._id}/deposit`).send({ amount: 150 }).expect(201);
    await client.post(`/wallets/${wallet.body._id}/withdraw`).send({ amount: 80 }).expect(201);

    const reconciliation = await client.get(`/wallets/${wallet.body._id}/reconcile`).expect(200);

    expect(reconciliation.body).toEqual({
      walletId: wallet.body._id,
      storedBalance: 370,
      ledgerBalance: 370,
      drift: 0,
      reconciled: true,
    });
  });

  it('detects drift when the stored balance is mutated out-of-band from the ledger', async () => {
    const wallet = await client
      .post('/wallets')
      .send({ userId: 'user-8', ownerName: 'Kojo Boateng' })
      .expect(201);

    await client.post(`/wallets/${wallet.body._id}/deposit`).send({ amount: 200 }).expect(201);

    const walletModel = getModel(app, Wallet.name);
    await walletModel.updateOne({ _id: wallet.body._id }, { $set: { balance: 250 } });

    const reconciliation = await client.get(`/wallets/${wallet.body._id}/reconcile`).expect(200);

    expect(reconciliation.body).toEqual({
      walletId: wallet.body._id,
      storedBalance: 250,
      ledgerBalance: 200,
      drift: 50,
      reconciled: false,
    });
  });

  it('exposes ledger-entry-level audit detail, paginated and filterable by direction', async () => {
    const wallet = await client
      .post('/wallets')
      .send({ userId: 'user-9', ownerName: 'Adwoa Frimpong' })
      .expect(201);

    await client.post(`/wallets/${wallet.body._id}/deposit`).send({ amount: 300 }).expect(201);
    await client.post(`/wallets/${wallet.body._id}/deposit`).send({ amount: 150 }).expect(201);
    await client.post(`/wallets/${wallet.body._id}/withdraw`).send({ amount: 80 }).expect(201);

    const fullAudit = await client.get(`/wallets/${wallet.body._id}/audit?limit=2`).expect(200);

    expect(fullAudit.body.total).toBe(3);
    expect(fullAudit.body.items).toHaveLength(2);
    // Newest first: the withdrawal (a DEBIT) was the most recent write.
    expect(fullAudit.body.items[0].direction).toBe('DEBIT');
    expect(fullAudit.body.items[0].amount).toBe(80);
    expect(fullAudit.body.items[0].balanceAfter).toBe(370);

    const creditsOnly = await client
      .get(`/wallets/${wallet.body._id}/audit?direction=CREDIT`)
      .expect(200);

    expect(creditsOnly.body.total).toBe(2);
    expect(creditsOnly.body.items.every((entry: any) => entry.direction === 'CREDIT')).toBe(true);
  });
});
