import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import * as amqp from 'amqp-connection-manager';
import { ChannelWrapper } from 'amqp-connection-manager';
import { ConfirmChannel } from 'amqplib';
import { v4 as uuidv4 } from 'uuid';

@Injectable()
export class RabbitMQService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(RabbitMQService.name);
  private connection: amqp.AmqpConnectionManager;
  private channelWrapper: ChannelWrapper;
  private readonly exchange: string;
  private readonly transferQueue: string;
  private readonly deadLetterExchange: string;
  private readonly deadLetterQueue: string;

  constructor(private readonly configService: ConfigService) {
    this.exchange = this.configService.getOrThrow<string>('rabbitmq.exchange');
    this.transferQueue = this.configService.getOrThrow<string>('rabbitmq.transferQueue');
    this.deadLetterExchange = this.configService.getOrThrow<string>('rabbitmq.deadLetterExchange');
    this.deadLetterQueue = this.configService.getOrThrow<string>('rabbitmq.deadLetterQueue');
  }

  async onModuleInit() {
    const uri = this.configService.getOrThrow<string>('rabbitmq.uri');
    this.connection = amqp.connect([uri]);
    this.connection.on('connectFailed', (err) =>
      this.logger.error(`RabbitMQ connection failed: ${err?.err?.message}`),
    );

    this.channelWrapper = this.connection.createChannel({
      json: true,
      setup: (channel: ConfirmChannel) =>
        Promise.all([
          channel.assertExchange(this.exchange, 'topic', { durable: true }),
          // Fanout: everything dead-lettered from the transfer queue (nacked
          // with requeue: false, or expired) lands in the one DLQ regardless
          // of its original routing key.
          channel.assertExchange(this.deadLetterExchange, 'fanout', { durable: true }),
          channel.assertQueue(this.deadLetterQueue, { durable: true }),
          channel.bindQueue(this.deadLetterQueue, this.deadLetterExchange, ''),
          channel.assertQueue(this.transferQueue, {
            durable: true,
            arguments: { 'x-dead-letter-exchange': this.deadLetterExchange },
          }),
          channel.bindQueue(this.transferQueue, this.exchange, 'transfer.*'),
        ]),
    });
  }

  async publish(
    routingKey: string,
    payload: Record<string, unknown>,
    correlationId?: string,
  ): Promise<void> {
    // Minted fresh on every call, not passed in: this marks "this specific
    // delivery attempt", not "this business entity". A real broker
    // redelivery of the same in-flight message reuses the same properties
    // (same messageId), while a genuinely new publish() call (a sweep
    // republish, a retried outbox relay) is a new delivery attempt and
    // correctly gets a new one - see InboxMessage/transfer-events.consumer.ts.
    const messageId = uuidv4();

    await this.channelWrapper.publish(this.exchange, routingKey, payload, {
      persistent: true,
      messageId,
      // A real AMQP 0-9-1 message property, not a custom header - lets a
      // consumer recover the id of whatever originated this event.
      ...(correlationId ? { correlationId } : {}),
    });
    this.logger.log(
      `Published event ${routingKey}${correlationId ? ` [correlationId=${correlationId}]` : ''}`,
    );
  }

  getChannelWrapper(): ChannelWrapper {
    return this.channelWrapper;
  }

  getTransferQueue(): string {
    return this.transferQueue;
  }

  getDeadLetterQueue(): string {
    return this.deadLetterQueue;
  }

  async onModuleDestroy() {
    await this.channelWrapper?.close();
    await this.connection?.close();
  }
}
