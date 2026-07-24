import { ConfigService } from '@nestjs/config';
import { Test, TestingModule } from '@nestjs/testing';
import * as amqp from 'amqp-connection-manager';
import { RabbitMQService } from './rabbitmq.service';

jest.mock('amqp-connection-manager', () => ({
  connect: jest.fn(),
}));

describe('RabbitMQService', () => {
  let service: RabbitMQService;
  let mockChannel: any;
  let mockChannelWrapper: any;
  let mockConnectionManager: any;
  let capturedSetup: (channel: unknown) => Promise<unknown>;

  const config: Record<string, string> = {
    'rabbitmq.uri': 'amqp://guest:guest@localhost:5672',
    'rabbitmq.exchange': 'wallet.events',
    'rabbitmq.transferQueue': 'transfer.events.queue',
    'rabbitmq.deadLetterExchange': 'wallet.events.dlx',
    'rabbitmq.deadLetterQueue': 'transfer.events.dlq',
  };

  beforeEach(async () => {
    mockChannel = {
      assertExchange: jest.fn().mockResolvedValue(undefined),
      assertQueue: jest.fn().mockResolvedValue(undefined),
      bindQueue: jest.fn().mockResolvedValue(undefined),
    };
    mockConnectionManager = {
      on: jest.fn(),
      createChannel: jest.fn((opts: { setup: (channel: unknown) => Promise<unknown> }) => {
        capturedSetup = opts.setup;
        mockChannelWrapper = { publish: jest.fn().mockResolvedValue(undefined), close: jest.fn() };
        return mockChannelWrapper;
      }),
      close: jest.fn(),
    };
    (amqp.connect as jest.Mock).mockReturnValue(mockConnectionManager);

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        RabbitMQService,
        {
          provide: ConfigService,
          useValue: { getOrThrow: jest.fn((key: string) => config[key]) },
        },
      ],
    }).compile();

    service = module.get(RabbitMQService);
    await service.onModuleInit();
    await capturedSetup(mockChannel);
  });

  afterEach(() => jest.clearAllMocks());

  it('asserts the main exchange and binds the transfer queue to it', () => {
    expect(mockChannel.assertExchange).toHaveBeenCalledWith('wallet.events', 'topic', {
      durable: true,
    });
    expect(mockChannel.bindQueue).toHaveBeenCalledWith(
      'transfer.events.queue',
      'wallet.events',
      'transfer.*',
    );
  });

  it('declares the transfer queue with a dead-letter exchange argument', () => {
    expect(mockChannel.assertQueue).toHaveBeenCalledWith('transfer.events.queue', {
      durable: true,
      arguments: { 'x-dead-letter-exchange': 'wallet.events.dlx' },
    });
  });

  it('asserts a fanout dead-letter exchange and binds the dead-letter queue to it', () => {
    expect(mockChannel.assertExchange).toHaveBeenCalledWith('wallet.events.dlx', 'fanout', {
      durable: true,
    });
    expect(mockChannel.assertQueue).toHaveBeenCalledWith('transfer.events.dlq', {
      durable: true,
    });
    expect(mockChannel.bindQueue).toHaveBeenCalledWith(
      'transfer.events.dlq',
      'wallet.events.dlx',
      '',
    );
  });

  it('exposes the dead-letter queue name', () => {
    expect(service.getDeadLetterQueue()).toBe('transfer.events.dlq');
  });

  describe('publish', () => {
    it('sets a fresh messageId string property on every call', async () => {
      await service.publish('transfer.initiated', { transferId: 't1' });
      await service.publish('transfer.initiated', { transferId: 't2' });

      const [firstCall, secondCall] = mockChannelWrapper.publish.mock.calls;
      expect(firstCall[3]).toEqual(expect.objectContaining({ messageId: expect.any(String) }));
      expect(secondCall[3]).toEqual(expect.objectContaining({ messageId: expect.any(String) }));
      expect(firstCall[3].messageId).not.toBe(secondCall[3].messageId);
    });

    it('sets both messageId and correlationId when a correlation id is provided', async () => {
      await service.publish('transfer.initiated', { transferId: 't1' }, 'corr-1');

      expect(mockChannelWrapper.publish).toHaveBeenCalledWith(
        'wallet.events',
        'transfer.initiated',
        { transferId: 't1' },
        expect.objectContaining({ messageId: expect.any(String), correlationId: 'corr-1' }),
      );
    });
  });
});
