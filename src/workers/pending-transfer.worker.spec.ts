import { ConfigService } from '@nestjs/config';
import { getModelToken } from '@nestjs/mongoose';
import { Test, TestingModule } from '@nestjs/testing';
import { Types } from 'mongoose';
import { RabbitMQService } from '../queue/rabbitmq.service';
import { Transfer } from '../wallets/schemas/transfer.schema';
import { PendingTransferWorker } from './pending-transfer.worker';

describe('PendingTransferWorker', () => {
  let worker: PendingTransferWorker;
  let transferModel: any;
  let rabbitMQService: any;

  beforeEach(async () => {
    transferModel = { find: jest.fn(), updateMany: jest.fn().mockResolvedValue(undefined) };
    rabbitMQService = { publish: jest.fn() };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        PendingTransferWorker,
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
        { provide: RabbitMQService, useValue: rabbitMQService },
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

  it('re-publishes transfer.initiated for each stale PENDING transfer, reconstructed from its own fields', async () => {
    const staleTransfer = makeTransfer({ amount: 42 });
    mockFind([staleTransfer]);

    await (worker as any).sweep();

    expect(rabbitMQService.publish).toHaveBeenCalledWith(
      'transfer.initiated',
      {
        transferId: staleTransfer.id,
        fromWalletId: staleTransfer.fromWalletId.toString(),
        toWalletId: staleTransfer.toWalletId.toString(),
        amount: 42,
      },
      undefined,
    );
  });

  it('propagates the originating correlation id when the stale transfer has one stored', async () => {
    const staleTransfer = makeTransfer({ correlationId: 'corr-stuck-1' });
    mockFind([staleTransfer]);

    await (worker as any).sweep();

    expect(rabbitMQService.publish).toHaveBeenCalledWith(
      'transfer.initiated',
      expect.any(Object),
      'corr-stuck-1',
    );
  });

  it('marks the whole batch as swept before attempting any publish', async () => {
    const first = makeTransfer();
    const second = makeTransfer();
    mockFind([first, second]);

    const callOrder: string[] = [];
    transferModel.updateMany.mockImplementation(async () => {
      callOrder.push('updateMany');
    });
    rabbitMQService.publish.mockImplementation(async () => {
      callOrder.push('publish');
    });

    await (worker as any).sweep();

    expect(transferModel.updateMany).toHaveBeenCalledWith(
      { _id: { $in: [first._id, second._id] } },
      { $set: { lastSweptAt: expect.any(Date) } },
    );
    expect(callOrder).toEqual(['updateMany', 'publish', 'publish']);
  });

  it('is a no-op when nothing is stale', async () => {
    mockFind([]);

    await (worker as any).sweep();

    expect(transferModel.updateMany).not.toHaveBeenCalled();
    expect(rabbitMQService.publish).not.toHaveBeenCalled();
  });

  it('keeps processing the rest when one republish fails', async () => {
    const first = makeTransfer();
    const second = makeTransfer();
    mockFind([first, second]);
    rabbitMQService.publish.mockRejectedValueOnce(new Error('broker unreachable'));
    rabbitMQService.publish.mockResolvedValueOnce(undefined);

    await (worker as any).sweep();

    expect(rabbitMQService.publish).toHaveBeenCalledTimes(2);
  });
});
