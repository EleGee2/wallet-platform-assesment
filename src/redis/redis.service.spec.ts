import { ConfigService } from '@nestjs/config';
import { Test, TestingModule } from '@nestjs/testing';
import Redis from 'ioredis';
import { RedisService } from './redis.service';

jest.mock('ioredis');

describe('RedisService', () => {
  let service: RedisService;
  let mockClient: {
    get: jest.Mock;
    set: jest.Mock;
    del: jest.Mock;
    on: jest.Mock;
    quit: jest.Mock;
  };

  beforeEach(async () => {
    mockClient = {
      get: jest.fn(),
      set: jest.fn(),
      del: jest.fn(),
      on: jest.fn(),
      quit: jest.fn(),
    };
    (Redis as unknown as jest.Mock).mockImplementation(() => mockClient);

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        RedisService,
        {
          provide: ConfigService,
          useValue: {
            get: jest.fn((key: string) => (key === 'redis.host' ? 'localhost' : 6379)),
            getOrThrow: jest.fn().mockReturnValue(3600),
          },
        },
      ],
    }).compile();

    service = module.get(RedisService);
  });

  afterEach(() => jest.clearAllMocks());

  describe('getCachedBalance', () => {
    it('returns the parsed balance on a cache hit', async () => {
      mockClient.get.mockResolvedValue('123.45');

      await expect(service.getCachedBalance('w1')).resolves.toBe(123.45);
      expect(mockClient.get).toHaveBeenCalledWith('wallet:balance:w1');
    });

    it('returns null on a cache miss', async () => {
      mockClient.get.mockResolvedValue(null);

      await expect(service.getCachedBalance('w1')).resolves.toBeNull();
    });

    it('degrades to a cache miss instead of throwing when Redis errors', async () => {
      mockClient.get.mockRejectedValue(new Error('connection refused'));

      await expect(service.getCachedBalance('w1')).resolves.toBeNull();
    });
  });

  describe('setCachedBalance', () => {
    it('sets the balance with the configured TTL', async () => {
      await service.setCachedBalance('w1', 250);

      expect(mockClient.set).toHaveBeenCalledWith('wallet:balance:w1', '250', 'EX', 3600);
    });

    it('does not throw when Redis errors', async () => {
      mockClient.set.mockRejectedValue(new Error('connection refused'));

      await expect(service.setCachedBalance('w1', 250)).resolves.toBeUndefined();
    });
  });

  describe('invalidateBalance', () => {
    it('deletes the cache key', async () => {
      await service.invalidateBalance('w1');

      expect(mockClient.del).toHaveBeenCalledWith('wallet:balance:w1');
    });

    it('does not throw when Redis errors', async () => {
      mockClient.del.mockRejectedValue(new Error('connection refused'));

      await expect(service.invalidateBalance('w1')).resolves.toBeUndefined();
    });
  });
});
