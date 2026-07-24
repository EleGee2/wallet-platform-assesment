import { getModelToken } from '@nestjs/mongoose';
import { Test, TestingModule } from '@nestjs/testing';
import { getCorrelationId } from '../common/context/request-context';
import { OutboxEvent, OutboxEventStatus } from './schemas/outbox-event.schema';
import { OutboxService } from './outbox.service';

jest.mock('../common/context/request-context', () => ({
  getCorrelationId: jest.fn(),
}));

describe('OutboxService', () => {
  let service: OutboxService;
  let outboxModel: any;

  beforeEach(async () => {
    outboxModel = {
      create: jest.fn(),
      find: jest.fn(),
      updateOne: jest.fn(),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        OutboxService,
        { provide: getModelToken(OutboxEvent.name), useValue: outboxModel },
      ],
    }).compile();

    service = module.get(OutboxService);
  });

  afterEach(() => jest.clearAllMocks());

  describe('enqueue', () => {
    it('stages the ambient correlation id on the created event', async () => {
      (getCorrelationId as jest.Mock).mockReturnValue('corr-xyz');
      const created = { _id: 'event-1' };
      outboxModel.create.mockResolvedValue([created]);

      const result = await service.enqueue('wallet.created', { walletId: 'w1' });

      expect(outboxModel.create).toHaveBeenCalledWith(
        [
          expect.objectContaining({
            routingKey: 'wallet.created',
            payload: { walletId: 'w1' },
            status: OutboxEventStatus.PENDING,
            correlationId: 'corr-xyz',
          }),
        ],
        undefined,
      );
      expect(result).toBe(created);
    });

    it('stores undefined when there is no ambient correlation id', async () => {
      (getCorrelationId as jest.Mock).mockReturnValue(undefined);
      outboxModel.create.mockResolvedValue([{ _id: 'event-2' }]);

      await service.enqueue('wallet.created', { walletId: 'w2' });

      expect(outboxModel.create).toHaveBeenCalledWith(
        [expect.objectContaining({ correlationId: undefined })],
        undefined,
      );
    });

    it('passes the session through when provided', async () => {
      const session = { id: 'session-1' } as any;
      outboxModel.create.mockResolvedValue([{ _id: 'event-3' }]);

      await service.enqueue('transfer.initiated', { transferId: 't1' }, session);

      expect(outboxModel.create).toHaveBeenCalledWith(expect.anything(), { session });
    });
  });

  describe('findPending', () => {
    it('queries pending events sorted oldest-first, bounded by the given limit', async () => {
      const exec = jest.fn().mockResolvedValue([]);
      const limit = jest.fn().mockReturnValue({ exec });
      const sort = jest.fn().mockReturnValue({ limit });
      outboxModel.find.mockReturnValue({ sort });

      await service.findPending(50);

      expect(outboxModel.find).toHaveBeenCalledWith({ status: OutboxEventStatus.PENDING });
      expect(sort).toHaveBeenCalledWith({ createdAt: 1 });
      expect(limit).toHaveBeenCalledWith(50);
    });
  });

  describe('markPublished', () => {
    it('marks the event published with a timestamp', async () => {
      await service.markPublished('event-1');

      expect(outboxModel.updateOne).toHaveBeenCalledWith(
        { _id: 'event-1' },
        expect.objectContaining({
          status: OutboxEventStatus.PUBLISHED,
          publishedAt: expect.any(Date),
        }),
      );
    });
  });
});
