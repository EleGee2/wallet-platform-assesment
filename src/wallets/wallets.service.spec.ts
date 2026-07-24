import { BadRequestException, ConflictException, NotFoundException } from '@nestjs/common';
import { getConnectionToken, getModelToken } from '@nestjs/mongoose';
import { Test, TestingModule } from '@nestjs/testing';
import { Types } from 'mongoose';
import { getCorrelationId } from '../common/context/request-context';
import { LedgerService } from '../ledger/ledger.service';
import { LedgerEntry } from '../ledger/schemas/ledger-entry.schema';
import { OutboxService } from '../outbox/outbox.service';
import { RedisService } from '../redis/redis.service';
import { Transaction, TransactionType } from '../transactions/schemas/transaction.schema';
import { TransactionsService } from '../transactions/transactions.service';
import { Transfer } from './schemas/transfer.schema';
import { Wallet } from './schemas/wallet.schema';
import { WalletsService } from './wallets.service';

jest.mock('../common/context/request-context', () => ({
  getCorrelationId: jest.fn(),
}));

describe('WalletsService', () => {
  let service: WalletsService;
  let walletModel: any;
  let transferModel: any;
  let transactionModel: any;
  let ledgerEntryModel: any;
  let transactionsService: any;
  let ledgerService: any;
  let outboxService: any;
  let redisService: any;

  const mockSession = {
    withTransaction: jest.fn(async (fn: () => Promise<unknown>) => fn()),
    endSession: jest.fn(),
  };

  beforeEach(async () => {
    walletModel = {
      create: jest.fn(),
      findById: jest.fn(),
      findByIdAndUpdate: jest.fn(),
      findOneAndUpdate: jest.fn(),
      exists: jest.fn(),
    };
    transferModel = {
      create: jest.fn(),
      findOne: jest.fn(),
    };
    transactionModel = {
      create: jest.fn(),
      find: jest.fn(),
      aggregate: jest.fn(),
    };
    ledgerEntryModel = {
      find: jest.fn(),
      countDocuments: jest.fn(),
    };
    transactionsService = { create: jest.fn(), findByReference: jest.fn() };
    ledgerService = {
      recordCredit: jest.fn(),
      recordDebit: jest.fn(),
      aggregateNetByWallet: jest.fn(),
    };
    outboxService = { enqueue: jest.fn() };
    redisService = {
      getCachedBalance: jest.fn(),
      setCachedBalance: jest.fn(),
      invalidateBalance: jest.fn(),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        WalletsService,
        {
          provide: getConnectionToken(),
          useValue: { startSession: jest.fn().mockResolvedValue(mockSession) },
        },
        { provide: getModelToken(Wallet.name), useValue: walletModel },
        { provide: getModelToken(Transfer.name), useValue: transferModel },
        { provide: getModelToken(Transaction.name), useValue: transactionModel },
        { provide: getModelToken(LedgerEntry.name), useValue: ledgerEntryModel },
        { provide: TransactionsService, useValue: transactionsService },
        { provide: LedgerService, useValue: ledgerService },
        { provide: OutboxService, useValue: outboxService },
        { provide: RedisService, useValue: redisService },
      ],
    }).compile();

    service = module.get(WalletsService);
  });

  afterEach(() => jest.clearAllMocks());

  describe('createWallet', () => {
    it('creates a wallet with a zero opening balance and enqueues a wallet.created event', async () => {
      const created = {
        _id: new Types.ObjectId(),
        userId: 'user-1',
        ownerName: 'Ama Owusu',
        balance: 0,
      };
      walletModel.create.mockResolvedValue([created]);

      const result = await service.createWallet({ userId: 'user-1', ownerName: 'Ama Owusu' });

      expect(walletModel.create).toHaveBeenCalledWith(
        [expect.objectContaining({ userId: 'user-1', balance: 0 })],
        expect.objectContaining({ session: mockSession }),
      );
      expect(outboxService.enqueue).toHaveBeenCalledWith(
        'wallet.created',
        expect.objectContaining({ walletId: created._id.toString() }),
        mockSession,
      );
      expect(result).toBe(created);
    });
  });

  describe('getWallet', () => {
    it('seeds the cache from Mongo on a cache miss', async () => {
      const wallet = {
        id: 'w1',
        _id: 'w1',
        balance: 250,
        toObject: () => ({ id: 'w1', balance: 250 }),
      };
      walletModel.findById.mockResolvedValue(wallet);
      redisService.getCachedBalance.mockResolvedValue(null);

      const result = await service.getWallet('w1');

      expect(redisService.setCachedBalance).toHaveBeenCalledWith('w1', 250);
      expect(result).toBe(wallet);
    });

    it('returns the cached balance instead of re-reading Mongo on a cache hit', async () => {
      const wallet = {
        id: 'w1',
        _id: 'w1',
        balance: 250,
        toObject: () => ({ id: 'w1', balance: 250 }),
      };
      walletModel.findById.mockResolvedValue(wallet);
      redisService.getCachedBalance.mockResolvedValue(99);

      const result = await service.getWallet('w1');

      expect(redisService.setCachedBalance).not.toHaveBeenCalled();
      expect(result).toEqual(expect.objectContaining({ balance: 99 }));
    });

    it('throws NotFoundException when the wallet does not exist', async () => {
      walletModel.findById.mockResolvedValue(null);

      await expect(service.getWallet('missing-id')).rejects.toThrow(NotFoundException);
    });
  });

  describe('deposit', () => {
    it('increments the balance atomically and records a ledger credit', async () => {
      const walletId = new Types.ObjectId().toString();
      const updatedWallet = { id: walletId, _id: walletId, balance: 150 };
      walletModel.findByIdAndUpdate.mockResolvedValue(updatedWallet);
      transactionsService.findByReference.mockResolvedValue(null);
      const transaction = { _id: new Types.ObjectId() };
      transactionsService.create.mockResolvedValue(transaction);

      const result = await service.deposit(walletId, { amount: 50 });

      expect(walletModel.findByIdAndUpdate).toHaveBeenCalledWith(
        walletId,
        { $inc: { balance: 50 } },
        { new: true, session: mockSession },
      );
      expect(transactionsService.create).toHaveBeenCalledWith(
        expect.objectContaining({ type: TransactionType.DEPOSIT, amount: 50 }),
        mockSession,
      );
      expect(ledgerService.recordCredit).toHaveBeenCalledWith(
        updatedWallet._id,
        transaction._id,
        50,
        150,
        mockSession,
      );
      expect(redisService.invalidateBalance).toHaveBeenCalledWith(walletId);
      expect(result).toBe(updatedWallet);
    });

    it('throws NotFoundException when the wallet does not exist', async () => {
      walletModel.findByIdAndUpdate.mockResolvedValue(null);

      await expect(service.deposit('missing-id', { amount: 10 })).rejects.toThrow(
        NotFoundException,
      );
      expect(redisService.invalidateBalance).not.toHaveBeenCalled();
    });

    it('skips the idempotency lookup when no reference is supplied', async () => {
      walletModel.findByIdAndUpdate.mockResolvedValue({ id: 'w1', _id: 'w1', balance: 150 });
      transactionsService.create.mockResolvedValue({ _id: new Types.ObjectId() });

      await service.deposit('w1', { amount: 50 });

      expect(transactionsService.findByReference).not.toHaveBeenCalled();
    });

    it('rejects a retried deposit that reuses a reference already recorded', async () => {
      transactionsService.findByReference.mockResolvedValue({ _id: new Types.ObjectId() });

      await expect(service.deposit('w1', { amount: 50, reference: 'dep-idem-1' })).rejects.toThrow(
        ConflictException,
      );

      expect(walletModel.findByIdAndUpdate).not.toHaveBeenCalled();
    });

    it('converts a duplicate-key race on the reference into a Conflict, not a raw Mongo error', async () => {
      walletModel.findByIdAndUpdate.mockResolvedValue({ id: 'w1', _id: 'w1', balance: 150 });
      transactionsService.findByReference.mockResolvedValue(null);
      transactionsService.create.mockRejectedValue(
        Object.assign(new Error('E11000 duplicate key'), { code: 11000 }),
      );

      await expect(service.deposit('w1', { amount: 50, reference: 'dep-idem-2' })).rejects.toThrow(
        ConflictException,
      );
    });
  });

  describe('withdraw', () => {
    it('atomically debits the wallet via a balance-guarded conditional update', async () => {
      const updatedWallet = { id: 'w1', _id: 'w1', balance: 60 };
      walletModel.findOneAndUpdate.mockResolvedValue(updatedWallet);
      transactionsService.findByReference.mockResolvedValue(null);
      const transaction = { _id: new Types.ObjectId() };
      transactionsService.create.mockResolvedValue(transaction);

      const result = await service.withdraw('w1', { amount: 40 });

      expect(walletModel.findOneAndUpdate).toHaveBeenCalledWith(
        { _id: 'w1', balance: { $gte: 40 } },
        { $inc: { balance: -40 } },
        { new: true, session: mockSession },
      );
      expect(ledgerService.recordDebit).toHaveBeenCalledWith(
        updatedWallet._id,
        transaction._id,
        40,
        60,
        mockSession,
      );
      expect(redisService.invalidateBalance).toHaveBeenCalledWith('w1');
      expect(result).toBe(updatedWallet);
    });

    it('rejects a withdrawal larger than the current balance without mutating it', async () => {
      walletModel.findOneAndUpdate.mockResolvedValue(null);
      walletModel.exists.mockResolvedValue(true);

      await expect(service.withdraw('w1', { amount: 40 })).rejects.toThrow(BadRequestException);
      expect(transactionsService.create).not.toHaveBeenCalled();
      expect(redisService.invalidateBalance).not.toHaveBeenCalled();
    });

    it('throws NotFoundException when the wallet does not exist', async () => {
      walletModel.findOneAndUpdate.mockResolvedValue(null);
      walletModel.exists.mockResolvedValue(null);

      await expect(service.withdraw('missing-id', { amount: 10 })).rejects.toThrow(
        NotFoundException,
      );
    });

    it('skips the idempotency lookup when no reference is supplied', async () => {
      walletModel.findOneAndUpdate.mockResolvedValue({ id: 'w1', _id: 'w1', balance: 60 });
      transactionsService.create.mockResolvedValue({ _id: new Types.ObjectId() });

      await service.withdraw('w1', { amount: 40 });

      expect(transactionsService.findByReference).not.toHaveBeenCalled();
    });

    it('rejects a retried withdrawal that reuses a reference already recorded', async () => {
      transactionsService.findByReference.mockResolvedValue({ _id: new Types.ObjectId() });

      await expect(service.withdraw('w1', { amount: 40, reference: 'idem-1' })).rejects.toThrow(
        ConflictException,
      );

      expect(walletModel.findOneAndUpdate).not.toHaveBeenCalled();
    });

    it('converts a duplicate-key race on the reference into a Conflict, not a raw Mongo error', async () => {
      walletModel.findOneAndUpdate.mockResolvedValue({ id: 'w1', _id: 'w1', balance: 60 });
      transactionsService.findByReference.mockResolvedValue(null);
      transactionsService.create.mockRejectedValue(
        Object.assign(new Error('E11000 duplicate key'), { code: 11000 }),
      );

      await expect(service.withdraw('w1', { amount: 40, reference: 'idem-2' })).rejects.toThrow(
        ConflictException,
      );
    });
  });

  describe('transfer', () => {
    const fromId = new Types.ObjectId();
    const toId = new Types.ObjectId();

    function mockWallets() {
      const fromWallet = { _id: fromId };
      const toWallet = { _id: toId };
      walletModel.findById.mockImplementation((id: unknown) => {
        if (String(id) === String(fromId)) return Promise.resolve(fromWallet);
        if (String(id) === String(toId)) return Promise.resolve(toWallet);
        return Promise.resolve(null);
      });
      return { fromWallet, toWallet };
    }

    it('rejects transfers between the same wallet', async () => {
      await expect(
        service.transfer({
          fromWalletId: fromId.toString(),
          toWalletId: fromId.toString(),
          amount: 10,
        }),
      ).rejects.toThrow(BadRequestException);
    });

    it('throws NotFoundException when either wallet is missing', async () => {
      walletModel.findById.mockResolvedValue(null);

      await expect(
        service.transfer({
          fromWalletId: fromId.toString(),
          toWalletId: toId.toString(),
          amount: 10,
        }),
      ).rejects.toThrow(NotFoundException);
    });

    it('rejects a transfer larger than the sender balance without mutating anything', async () => {
      mockWallets();
      walletModel.findOneAndUpdate.mockResolvedValue(null);

      await expect(
        service.transfer({
          fromWalletId: fromId.toString(),
          toWalletId: toId.toString(),
          amount: 10,
        }),
      ).rejects.toThrow(BadRequestException);
      expect(transferModel.create).not.toHaveBeenCalled();
      expect(redisService.invalidateBalance).not.toHaveBeenCalled();
    });

    it('atomically debits the sender, records a ledger entry, and stages a transfer.initiated outbox event', async () => {
      (getCorrelationId as jest.Mock).mockReturnValue('corr-transfer-1');
      const { fromWallet, toWallet } = mockWallets();
      const updatedFromWallet = { _id: fromId, balance: 70 };
      walletModel.findOneAndUpdate.mockResolvedValue(updatedFromWallet);
      const createdTransfer = { _id: new Types.ObjectId(), status: 'PENDING' };
      transferModel.create.mockResolvedValue([createdTransfer]);
      const debitTransaction = { _id: new Types.ObjectId() };
      transactionModel.create.mockResolvedValue([debitTransaction]);

      const result = await service.transfer({
        fromWalletId: fromId.toString(),
        toWalletId: toId.toString(),
        amount: 30,
      });

      expect(walletModel.findOneAndUpdate).toHaveBeenCalledWith(
        { _id: fromWallet._id, balance: { $gte: 30 } },
        { $inc: { balance: -30 } },
        { new: true, session: mockSession },
      );
      expect(transferModel.create).toHaveBeenCalledWith(
        [expect.objectContaining({ correlationId: 'corr-transfer-1' })],
        { session: mockSession },
      );
      expect(ledgerService.recordDebit).toHaveBeenCalledWith(
        updatedFromWallet._id,
        debitTransaction._id,
        30,
        70,
        mockSession,
      );
      expect(outboxService.enqueue).toHaveBeenCalledWith(
        'transfer.initiated',
        expect.objectContaining({
          transferId: createdTransfer._id.toString(),
          toWalletId: toWallet._id.toString(),
          amount: 30,
        }),
        mockSession,
      );
      expect(redisService.invalidateBalance).toHaveBeenCalledWith(fromId.toString());
      expect(result).toBe(createdTransfer);
    });

    it('returns the original transfer on retry with the same idempotency key, without a second debit', async () => {
      mockWallets();
      const createdTransfer = { _id: new Types.ObjectId(), status: 'PENDING' };
      transferModel.findOne.mockResolvedValueOnce(null).mockResolvedValueOnce(createdTransfer);
      walletModel.findOneAndUpdate.mockResolvedValue({ _id: fromId, balance: 70 });
      transferModel.create.mockResolvedValue([createdTransfer]);
      transactionModel.create.mockResolvedValue([{ _id: new Types.ObjectId() }]);

      const dto = {
        fromWalletId: fromId.toString(),
        toWalletId: toId.toString(),
        amount: 30,
        idempotencyKey: 'retry-key-1',
      };

      const first = await service.transfer(dto);
      const second = await service.transfer(dto);

      expect(transferModel.create).toHaveBeenCalledTimes(1);
      expect(walletModel.findOneAndUpdate).toHaveBeenCalledTimes(1);
      // The replay short-circuits before any write, so only the first (real)
      // debit should have invalidated the cache - not the replay.
      expect(redisService.invalidateBalance).toHaveBeenCalledTimes(1);
      expect(second).toBe(createdTransfer);
      expect(second).toBe(first);
    });

    it('resolves to the winning transfer when a concurrent request wins a race on the same idempotency key', async () => {
      mockWallets();
      walletModel.findOneAndUpdate.mockResolvedValue({ _id: fromId, balance: 70 });
      const winningTransfer = { _id: new Types.ObjectId(), status: 'PENDING' };
      transferModel.create.mockRejectedValue(
        Object.assign(new Error('E11000 duplicate key'), { code: 11000 }),
      );
      transferModel.findOne.mockResolvedValueOnce(null).mockResolvedValueOnce(winningTransfer);

      const result = await service.transfer({
        fromWalletId: fromId.toString(),
        toWalletId: toId.toString(),
        amount: 30,
        idempotencyKey: 'retry-key-2',
      });

      expect(result).toBe(winningTransfer);
      expect(outboxService.enqueue).not.toHaveBeenCalled();
      // Redundant but harmless - the losing attempt's own debit rolled back,
      // but invalidation is idempotent, so it's still called defensively.
      expect(redisService.invalidateBalance).toHaveBeenCalledWith(fromId.toString());
    });

    it('ends the Mongo session even when the transaction fails partway through', async () => {
      mockWallets();
      walletModel.findOneAndUpdate.mockResolvedValue({ _id: fromId, balance: 70 });
      const createdTransfer = { _id: new Types.ObjectId(), status: 'PENDING' };
      transferModel.create.mockResolvedValue([createdTransfer]);
      transactionModel.create.mockRejectedValue(new Error('write conflict'));

      await expect(
        service.transfer({
          fromWalletId: fromId.toString(),
          toWalletId: toId.toString(),
          amount: 30,
        }),
      ).rejects.toThrow('write conflict');

      expect(mockSession.endSession).toHaveBeenCalled();
      expect(outboxService.enqueue).not.toHaveBeenCalled();
      expect(redisService.invalidateBalance).not.toHaveBeenCalled();
    });
  });

  describe('getDashboard', () => {
    function mockWalletLookup(wallet: unknown) {
      walletModel.findById.mockReturnValue({
        lean: () => ({ exec: () => Promise.resolve(wallet) }),
      });
    }

    function mockAggregate(stats: unknown[]) {
      transactionModel.aggregate.mockResolvedValue(stats);
    }

    function mockRecentTransactions(transactions: unknown[]) {
      const exec = jest.fn().mockResolvedValue(transactions);
      const lean = jest.fn().mockReturnValue({ exec });
      const limit = jest.fn().mockReturnValue({ lean });
      const sort = jest.fn().mockReturnValue({ limit });
      transactionModel.find.mockReturnValue({ sort });
      return { sort, limit };
    }

    function mockLedgerEntries(entries: unknown[]) {
      ledgerEntryModel.find.mockReturnValue({
        lean: () => ({ exec: () => Promise.resolve(entries) }),
      });
    }

    it('computes totals via an ObjectId-cast aggregation and batches ledger entries for only the recent transactions', async () => {
      const walletId = new Types.ObjectId().toString();
      mockWalletLookup({ _id: walletId, balance: 500 });
      mockAggregate([{ transactionCount: 15, totalDeposited: 800, totalWithdrawn: 300 }]);

      const txn1 = { _id: new Types.ObjectId(), type: TransactionType.DEPOSIT, amount: 100 };
      const txn2 = { _id: new Types.ObjectId(), type: TransactionType.WITHDRAWAL, amount: 50 };
      const { sort, limit } = mockRecentTransactions([txn1, txn2]);

      const entry1 = { transactionId: txn1._id, direction: 'CREDIT' };
      const entry2 = { transactionId: txn2._id, direction: 'DEBIT' };
      mockLedgerEntries([entry1, entry2]);

      const result = await service.getDashboard(walletId);

      expect(transactionModel.aggregate).toHaveBeenCalledWith([
        expect.objectContaining({
          $match: { walletId: expect.any(Types.ObjectId) },
        }),
        expect.anything(),
      ]);
      const [[pipeline]] = transactionModel.aggregate.mock.calls;
      expect(pipeline[0].$match.walletId.toString()).toBe(walletId);

      expect(sort).toHaveBeenCalledWith({ createdAt: -1 });
      expect(limit).toHaveBeenCalledWith(10);

      expect(ledgerEntryModel.find).toHaveBeenCalledWith({
        transactionId: { $in: [txn1._id, txn2._id] },
      });

      expect(result.totalDeposited).toBe(800);
      expect(result.totalWithdrawn).toBe(300);
      expect(result.transactionCount).toBe(15);
      expect(result.recentActivity).toEqual([
        { transaction: txn1, entries: [entry1] },
        { transaction: txn2, entries: [entry2] },
      ]);
    });

    it('throws NotFoundException when the wallet does not exist', async () => {
      mockWalletLookup(null);

      await expect(service.getDashboard('missing-id')).rejects.toThrow(NotFoundException);
    });

    it('defaults totals and count to zero for a wallet with no transactions', async () => {
      const walletId = new Types.ObjectId().toString();
      mockWalletLookup({ _id: walletId, balance: 0 });
      mockAggregate([]);
      mockRecentTransactions([]);
      mockLedgerEntries([]);

      const result = await service.getDashboard(walletId);

      expect(result.totalDeposited).toBe(0);
      expect(result.totalWithdrawn).toBe(0);
      expect(result.transactionCount).toBe(0);
      expect(result.recentActivity).toEqual([]);
    });

    it('reports the full lifetime transaction count even though recentActivity is capped at 10', async () => {
      const walletId = new Types.ObjectId().toString();
      mockWalletLookup({ _id: walletId, balance: 1000 });
      mockAggregate([{ transactionCount: 37, totalDeposited: 5000, totalWithdrawn: 4000 }]);
      const tenTransactions = Array.from({ length: 10 }, (_, i) => ({
        _id: new Types.ObjectId(),
        type: TransactionType.DEPOSIT,
        amount: i + 1,
      }));
      mockRecentTransactions(tenTransactions);
      mockLedgerEntries([]);

      const result = await service.getDashboard(walletId);

      expect(result.transactionCount).toBe(37);
      expect(result.recentActivity).toHaveLength(10);
    });
  });

  describe('reconcileWallet', () => {
    function mockWalletLookup(wallet: unknown) {
      walletModel.findById.mockReturnValue({
        lean: () => ({ exec: () => Promise.resolve(wallet) }),
      });
    }

    it('reports reconciled when the stored balance matches the ledger-derived total', async () => {
      const walletId = new Types.ObjectId().toString();
      mockWalletLookup({ _id: walletId, balance: 500 });
      ledgerService.aggregateNetByWallet.mockResolvedValue(500);

      const result = await service.reconcileWallet(walletId);

      expect(ledgerService.aggregateNetByWallet).toHaveBeenCalledWith(walletId);
      expect(result).toEqual({
        walletId,
        storedBalance: 500,
        ledgerBalance: 500,
        drift: 0,
        reconciled: true,
      });
    });

    it('reports the signed drift when the stored balance disagrees with the ledger', async () => {
      const walletId = new Types.ObjectId().toString();
      mockWalletLookup({ _id: walletId, balance: 550 });
      ledgerService.aggregateNetByWallet.mockResolvedValue(500);

      const result = await service.reconcileWallet(walletId);

      expect(result).toEqual({
        walletId,
        storedBalance: 550,
        ledgerBalance: 500,
        drift: 50,
        reconciled: false,
      });
    });

    it('throws NotFoundException when the wallet does not exist', async () => {
      mockWalletLookup(null);

      await expect(service.reconcileWallet('missing-wallet')).rejects.toThrow(NotFoundException);
      expect(ledgerService.aggregateNetByWallet).not.toHaveBeenCalled();
    });
  });

  describe('getAudit', () => {
    function mockWalletLookup(wallet: unknown) {
      walletModel.findById.mockReturnValue({
        lean: () => ({ exec: () => Promise.resolve(wallet) }),
      });
    }

    function mockEntries(entries: unknown[]) {
      const exec = jest.fn().mockResolvedValue(entries);
      const lean = jest.fn().mockReturnValue({ exec });
      const limit = jest.fn().mockReturnValue({ lean });
      const skip = jest.fn().mockReturnValue({ limit });
      const sort = jest.fn().mockReturnValue({ skip });
      ledgerEntryModel.find.mockReturnValue({ sort });
      return { sort, skip, limit };
    }

    it('paginates ledger entries newest-first and reports the total count', async () => {
      const walletId = new Types.ObjectId().toString();
      mockWalletLookup({ _id: walletId });
      const entries = [{ direction: 'CREDIT', amount: 100, balanceAfter: 100 }];
      const { sort, skip, limit } = mockEntries(entries);
      ledgerEntryModel.countDocuments.mockResolvedValue(37);

      const result = await service.getAudit(walletId, { page: 2, limit: 10 });

      expect(ledgerEntryModel.find).toHaveBeenCalledWith({ walletId });
      expect(sort).toHaveBeenCalledWith({ createdAt: -1 });
      expect(skip).toHaveBeenCalledWith(10);
      expect(limit).toHaveBeenCalledWith(10);
      expect(result).toEqual({ items: entries, total: 37, page: 2, limit: 10 });
    });

    it('applies the optional direction filter', async () => {
      const walletId = new Types.ObjectId().toString();
      mockWalletLookup({ _id: walletId });
      mockEntries([]);
      ledgerEntryModel.countDocuments.mockResolvedValue(0);

      await service.getAudit(walletId, { direction: 'DEBIT' as any, page: 1, limit: 20 });

      expect(ledgerEntryModel.find).toHaveBeenCalledWith({ walletId, direction: 'DEBIT' });
      expect(ledgerEntryModel.countDocuments).toHaveBeenCalledWith({
        walletId,
        direction: 'DEBIT',
      });
    });

    it('returns an empty page for a wallet with no ledger entries', async () => {
      const walletId = new Types.ObjectId().toString();
      mockWalletLookup({ _id: walletId });
      mockEntries([]);
      ledgerEntryModel.countDocuments.mockResolvedValue(0);

      const result = await service.getAudit(walletId, { page: 1, limit: 20 });

      expect(result).toEqual({ items: [], total: 0, page: 1, limit: 20 });
    });

    it('throws NotFoundException when the wallet does not exist', async () => {
      mockWalletLookup(null);

      await expect(service.getAudit('missing-wallet', { page: 1, limit: 20 })).rejects.toThrow(
        NotFoundException,
      );
      expect(ledgerEntryModel.find).not.toHaveBeenCalled();
    });
  });
});
