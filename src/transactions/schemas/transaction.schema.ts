import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { HydratedDocument, Schema as MongooseSchema, Types } from 'mongoose';

export type TransactionDocument = HydratedDocument<Transaction>;

export enum TransactionType {
  DEPOSIT = 'DEPOSIT',
  WITHDRAWAL = 'WITHDRAWAL',
  TRANSFER_IN = 'TRANSFER_IN',
  TRANSFER_OUT = 'TRANSFER_OUT',
}

export enum TransactionStatus {
  PENDING = 'PENDING',
  COMPLETED = 'COMPLETED',
  FAILED = 'FAILED',
}

@Schema({ timestamps: true, collection: 'transactions' })
export class Transaction {
  @Prop({ type: MongooseSchema.Types.ObjectId, ref: 'Wallet', required: true })
  walletId: Types.ObjectId;

  @Prop({ type: String, enum: TransactionType, required: true })
  type: TransactionType;

  @Prop({ required: true })
  amount: number;

  @Prop({ type: String, enum: TransactionStatus, default: TransactionStatus.COMPLETED })
  status: TransactionStatus;

  @Prop()
  balanceAfter?: number;

  // Idempotency key supplied by the caller, or derived for internally generated transactions.
  // Unique + sparse: enforces at-most-one transaction per reference at the database
  // level (the real concurrency backstop for retried requests), while allowing any
  // number of transactions that don't supply one.
  @Prop({ unique: true, sparse: true })
  reference?: string;

  @Prop({ type: MongooseSchema.Types.ObjectId, ref: 'Transfer' })
  transferId?: Types.ObjectId;

  @Prop({ type: MongooseSchema.Types.ObjectId, ref: 'Wallet' })
  counterpartyWalletId?: Types.ObjectId;

  createdAt?: Date;
  updatedAt?: Date;
}

export const TransactionSchema = SchemaFactory.createForClass(Transaction);

TransactionSchema.index({ walletId: 1, createdAt: -1 });
