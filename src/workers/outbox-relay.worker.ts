import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { OutboxService } from '../outbox/outbox.service';
import { RabbitMQService } from '../queue/rabbitmq.service';

@Injectable()
export class OutboxRelayWorker implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(OutboxRelayWorker.name);
  private timer: NodeJS.Timeout;
  private running = false;

  constructor(
    private readonly outboxService: OutboxService,
    private readonly rabbitMQService: RabbitMQService,
    private readonly configService: ConfigService,
  ) {}

  onModuleInit() {
    const intervalMs = this.configService.getOrThrow<number>('workers.outboxRelayIntervalMs');
    this.timer = setInterval(() => this.relay(), intervalMs);
  }

  private async relay() {
    if (this.running) {
      return;
    }
    this.running = true;

    try {
      const pending = await this.outboxService.findPending(50);
      for (const event of pending) {
        await this.rabbitMQService.publish(event.routingKey, event.payload, event.correlationId);
        await this.outboxService.markPublished(event.id);
      }
    } catch (error) {
      this.logger.error(`Outbox relay failed: ${(error as Error).message}`);
    } finally {
      this.running = false;
    }
  }

  onModuleDestroy() {
    clearInterval(this.timer);
  }
}
