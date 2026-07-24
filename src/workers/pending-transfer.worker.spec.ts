import { ConfigService } from '@nestjs/config';
import { getConnectionToken, getModelToken } from '@nestjs/mongoose';
import { Test, TestingModule } from '@nestjs/testing';
import { Types } from 'mongoose';
import { getCorrelationId } from '../common/context/request-context';
import { OutboxService } from '../outbox/outbox.service';
import { Transfer } from '../wallets/schemas/transfer.schema';
import { PendingTransferWorker } from './pending-transfer.worker';

describe('PendingTransferWorker', () => {
  let worker: PendingTransferWorker;
  let transferModel: any;
  let outboxService: any;
  let connection: any;

  function mockNormalSession() {
    return {
      withTransaction: jest.fn(async (fn: () => Promise<unknown>) => fn()),
      endSession: jest.fn(),
    };
  }

  beforeEach(async () => {
    transferModel = {
      find: jest.fn(),
      updateOne: jest.fn().mockResolvedValue({ matchedCount: 1 }),
    };
    outboxService = { enqueue: jest.fn() };
    connection = { startSession: jest.fn().mockImplementation(async () => mockNormalSession()) };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        PendingTransferWorker,
        { provide: getConnectionToken(), useValue: connection },
        { provide: getModelToken(Transfer.name), useValue: transferModel },
        {
          provide: ConfigService,
          useValue: {
            getOrThrow: jest.fn((key: string) => {
              if (key === 'workers.pendingTransferSweepIntervalMs') return 5000;
              if (key === 'workers.pendingTransferTimeoutMs') return 60000;
              throw new Error(`unexpected config key ${key}`);
            }),
          },
        },
        { provide: OutboxService, useValue: outboxService },
      ],
    }).compile();

    worker = module.get(PendingTransferWorker);
  });

  afterEach(() => jest.clearAllMocks());

  function makeTransfer(overrides: Partial<Record<string, unknown>> = {}) {
    const _id = new Types.ObjectId();
    return {
      _id,
      id: _id.toString(),
      fromWalletId: new Types.ObjectId(),
      toWalletId: new Types.ObjectId(),
      amount: 42,
      ...overrides,
    };
  }

  function mockFind(transfers: unknown[]) {
    const exec = jest.fn().mockResolvedValue(transfers);
    const limit = jest.fn().mockReturnValue({ exec });
    transferModel.find.mockReturnValue({ limit });
    return { limit, exec };
  }

  it('queries for stale, not-recently-swept PENDING transfers with a bounded batch size', async () => {
    const { limit } = mockFind([]);

    await (worker as any).sweep();

    expect(transferModel.find).toHaveBeenCalledWith(
      expect.objectContaining({
        status: 'PENDING',
        createdAt: { $lt: expect.any(Date) },
        $or: [{ lastSweptAt: { $exists: false } }, { lastSweptAt: { $lt: expect.any(Date) } }],
      }),
    );
    expect(limit).toHaveBeenCalledWith(100);
  });

  it('atomically marks a stale transfer swept and stages its republish in the outbox', async () => {
    const staleTransfer = makeTransfer({ amount: 42 });
    mockFind([staleTransfer]);

    await (worker as any).sweep();

    expect(transferModel.updateOne).toHaveBeenCalledWith(
      { _id: staleTransfer._id, status: 'PENDING' },
      { $set: { lastSweptAt: expect.any(Date) } },
      { session: expect.anything() },
    );
    expect(outboxService.enqueue).toHaveBeenCalledWith(
      'transfer.initiated',
      {
        transferId: staleTransfer.id,
        fromWalletId: staleTransfer.fromWalletId.toString(),
        toWalletId: staleTransfer.toWalletId.toString(),
        amount: 42,
      },
      expect.anything(),
    );
  });

  it('propagates the originating correlation id via the ambient request context', async () => {
    const staleTransfer = makeTransfer({ correlationId: 'corr-stuck-1' });
    mockFind([staleTransfer]);

    let capturedDuringEnqueue: string | undefined;
    outboxService.enqueue.mockImplementation(async () => {
      capturedDuringEnqueue = getCorrelationId();
    });

    await (worker as any).sweep();

    expect(capturedDuringEnqueue).toBe('corr-stuck-1');
  });

  it('skips staging a republish when the transfer was no longer PENDING at update time', async () => {
    const staleTransfer = makeTransfer();
    mockFind([staleTransfer]);
    transferModel.updateOne.mockResolvedValueOnce({ matchedCount: 0 });

    await (worker as any).sweep();

    expect(outboxService.enqueue).not.toHaveBeenCalled();
  });

  it('is a no-op when nothing is stale', async () => {
    mockFind([]);

    await (worker as any).sweep();

    expect(transferModel.updateOne).not.toHaveBeenCalled();
    expect(outboxService.enqueue).not.toHaveBeenCalled();
  });

  it('keeps processing the rest when one transfer fails to stage', async () => {
    const first = makeTransfer();
    const second = makeTransfer();
    mockFind([first, second]);

    connection.startSession
      .mockImplementationOnce(async () => ({
        withTransaction: jest.fn().mockRejectedValue(new Error('write conflict')),
        endSession: jest.fn(),
      }))
      .mockImplementationOnce(async () => mockNormalSession());

    await (worker as any).sweep();

    expect(outboxService.enqueue).toHaveBeenCalledTimes(1);
  });
});
