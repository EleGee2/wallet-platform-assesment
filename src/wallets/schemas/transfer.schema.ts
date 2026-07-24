import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { HydratedDocument, Schema as MongooseSchema, Types } from 'mongoose';

export type TransferDocument = HydratedDocument<Transfer>;

export enum TransferStatus {
  PENDING = 'PENDING',
  COMPLETED = 'COMPLETED',
  FAILED = 'FAILED',
}

@Schema({ timestamps: true, collection: 'transfers' })
export class Transfer {
  @Prop({ type: MongooseSchema.Types.ObjectId, ref: 'Wallet', required: true })
  fromWalletId: Types.ObjectId;

  @Prop({ type: MongooseSchema.Types.ObjectId, ref: 'Wallet', required: true })
  toWalletId: Types.ObjectId;

  @Prop({ required: true })
  amount: number;

  @Prop({ type: String, enum: TransferStatus, default: TransferStatus.PENDING })
  status: TransferStatus;

  // Idempotency key supplied by the caller. Unique + sparse: enforces at-most-one
  // transfer per key at the database level (the real concurrency backstop for a
  // racing retry), while allowing any number of transfers that don't supply one.
  @Prop({ unique: true, sparse: true })
  idempotencyKey?: string;

  @Prop()
  failureReason?: string;

  // Set by PendingTransferWorker each time it re-publishes a stale transfer,
  // so a still-stuck (or genuinely poison) transfer is retried once per
  // timeout window instead of on every sweep tick.
  @Prop()
  lastSweptAt?: Date;

  // The correlation ID of the HTTP request that created this transfer, if
  // any - stored here (not just on the initial outbox event) so the sweep
  // worker can carry it forward too when it reconstructs a republish from
  // this document's own fields.
  @Prop()
  correlationId?: string;

  createdAt?: Date;
  updatedAt?: Date;
}

export const TransferSchema = SchemaFactory.createForClass(Transfer);
