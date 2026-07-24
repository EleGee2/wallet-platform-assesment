import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { HydratedDocument } from 'mongoose';

export type InboxMessageDocument = HydratedDocument<InboxMessage>;

// One row per AMQP message actually processed - claimed atomically before
// any business logic runs, so a redelivery of the same message can't be
// reprocessed even if the consuming code has no natural status field of its
// own to guard on. See transfer-events.consumer.ts for how this is used
// alongside (not instead of) the transfer's own status guard.
@Schema({ timestamps: true, collection: 'inbox_messages' })
export class InboxMessage {
  @Prop({ required: true, unique: true })
  messageId: string;

  createdAt?: Date;
  updatedAt?: Date;
}

export const InboxMessageSchema = SchemaFactory.createForClass(InboxMessage);
