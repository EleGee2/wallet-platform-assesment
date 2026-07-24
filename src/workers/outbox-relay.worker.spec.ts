import { ConfigService } from '@nestjs/config';
import { Test, TestingModule } from '@nestjs/testing';
import { OutboxService } from '../outbox/outbox.service';
import { RabbitMQService } from '../queue/rabbitmq.service';
import { OutboxRelayWorker } from './outbox-relay.worker';

describe('OutboxRelayWorker', () => {
  let worker: OutboxRelayWorker;
  let outboxService: any;
  let rabbitMQService: any;

  beforeEach(async () => {
    outboxService = { findPending: jest.fn(), markPublished: jest.fn() };
    rabbitMQService = { publish: jest.fn() };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        OutboxRelayWorker,
        { provide: OutboxService, useValue: outboxService },
        { provide: RabbitMQService, useValue: rabbitMQService },
        {
          provide: ConfigService,
          useValue: { getOrThrow: jest.fn().mockReturnValue(2000) },
        },
      ],
    }).compile();

    worker = module.get(OutboxRelayWorker);
  });

  afterEach(() => jest.clearAllMocks());

  it('propagates each event correlation id through to the publish call', async () => {
    const events = [
      { id: 'e1', routingKey: 'wallet.created', payload: { a: 1 }, correlationId: 'corr-1' },
      { id: 'e2', routingKey: 'transfer.initiated', payload: { b: 2 }, correlationId: undefined },
    ];
    outboxService.findPending.mockResolvedValue(events);

    await (worker as any).relay();

    expect(rabbitMQService.publish).toHaveBeenNthCalledWith(
      1,
      'wallet.created',
      { a: 1 },
      'corr-1',
    );
    expect(rabbitMQService.publish).toHaveBeenNthCalledWith(
      2,
      'transfer.initiated',
      { b: 2 },
      undefined,
    );
    expect(outboxService.markPublished).toHaveBeenCalledWith('e1');
    expect(outboxService.markPublished).toHaveBeenCalledWith('e2');
  });

  it('is a no-op when nothing is pending', async () => {
    outboxService.findPending.mockResolvedValue([]);

    await (worker as any).relay();

    expect(rabbitMQService.publish).not.toHaveBeenCalled();
    expect(outboxService.markPublished).not.toHaveBeenCalled();
  });

  it('logs and recovers when the relay batch throws', async () => {
    outboxService.findPending.mockRejectedValue(new Error('mongo down'));

    await expect((worker as any).relay()).resolves.toBeUndefined();
  });
});
