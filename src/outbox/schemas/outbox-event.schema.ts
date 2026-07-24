import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { HydratedDocument } from 'mongoose';

export type OutboxEventDocument = HydratedDocument<OutboxEvent>;

export enum OutboxEventStatus {
  PENDING = 'PENDING',
  PUBLISHED = 'PUBLISHED',
}

@Schema({ timestamps: true, collection: 'outbox_events' })
export class OutboxEvent {
  @Prop({ required: true })
  routingKey: string;

  @Prop({ type: Object, required: true })
  payload: Record<string, unknown>;

  @Prop({ type: String, enum: OutboxEventStatus, default: OutboxEventStatus.PENDING })
  status: OutboxEventStatus;

  @Prop()
  publishedAt?: Date;

  // The correlation ID of the HTTP request that staged this event, if any -
  // carried forward as the AMQP message's own correlationId property when
  // OutboxRelayWorker publishes it, so a request can be traced into its
  // downstream consumer.
  @Prop()
  correlationId?: string;

  createdAt?: Date;
  updatedAt?: Date;
}

export const OutboxEventSchema = SchemaFactory.createForClass(OutboxEvent);

OutboxEventSchema.index({ status: 1, createdAt: 1 });
