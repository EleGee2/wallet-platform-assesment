import { Injectable } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { ClientSession, Model } from 'mongoose';
import { getCorrelationId } from '../common/context/request-context';
import { OutboxEvent, OutboxEventDocument, OutboxEventStatus } from './schemas/outbox-event.schema';

@Injectable()
export class OutboxService {
  constructor(
    @InjectModel(OutboxEvent.name) private readonly outboxModel: Model<OutboxEventDocument>,
  ) {}

  async enqueue(routingKey: string, payload: Record<string, unknown>, session?: ClientSession) {
    const [event] = await this.outboxModel.create(
      [
        {
          routingKey,
          payload,
          status: OutboxEventStatus.PENDING,
          correlationId: getCorrelationId(),
        },
      ],
      session ? { session } : undefined,
    );
    return event;
  }

  async findPending(limit: number) {
    return this.outboxModel
      .find({ status: OutboxEventStatus.PENDING })
      .sort({ createdAt: 1 })
      .limit(limit)
      .exec();
  }

  async markPublished(id: string) {
    await this.outboxModel.updateOne(
      { _id: id },
      { status: OutboxEventStatus.PUBLISHED, publishedAt: new Date() },
    );
  }
}
