import { Injectable, Logger, OnModuleDestroy } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import Redis from 'ioredis';

@Injectable()
export class RedisService implements OnModuleDestroy {
  private readonly logger = new Logger(RedisService.name);
  private readonly client: Redis;
  private readonly ttlSeconds: number;

  constructor(private readonly configService: ConfigService) {
    this.client = new Redis({
      host: this.configService.get<string>('redis.host'),
      port: this.configService.get<number>('redis.port'),
      lazyConnect: false,
    });
    this.ttlSeconds = this.configService.getOrThrow<number>('redis.ttlSeconds');

    this.client.on('error', (err) => this.logger.error(`Redis error: ${err.message}`));
  }

  private walletBalanceKey(walletId: string): string {
    return `wallet:balance:${walletId}`;
  }

  // Cache reads/writes degrade gracefully instead of throwing: a Redis outage
  // should never turn an otherwise-successful Mongo read or write into a
  // client-facing error - callers just get treated to a cache miss / no-op.
  async getCachedBalance(walletId: string): Promise<number | null> {
    try {
      const value = await this.client.get(this.walletBalanceKey(walletId));
      return value === null ? null : parseFloat(value);
    } catch (error) {
      this.logger.error(
        `Failed to read cached balance for wallet ${walletId}: ${(error as Error).message}`,
      );
      return null;
    }
  }

  async setCachedBalance(walletId: string, balance: number): Promise<void> {
    try {
      await this.client.set(
        this.walletBalanceKey(walletId),
        balance.toString(),
        'EX',
        this.ttlSeconds,
      );
    } catch (error) {
      this.logger.error(
        `Failed to cache balance for wallet ${walletId}: ${(error as Error).message}`,
      );
    }
  }

  async invalidateBalance(walletId: string): Promise<void> {
    try {
      await this.client.del(this.walletBalanceKey(walletId));
    } catch (error) {
      this.logger.error(
        `Failed to invalidate cached balance for wallet ${walletId}: ${(error as Error).message}`,
      );
    }
  }

  getClient(): Redis {
    return this.client;
  }

  async onModuleDestroy() {
    await this.client.quit();
  }
}
