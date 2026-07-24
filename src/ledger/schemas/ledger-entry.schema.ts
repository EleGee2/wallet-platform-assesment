import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { HydratedDocument, Schema as MongooseSchema, Types } from 'mongoose';

export type LedgerEntryDocument = HydratedDocument<LedgerEntry>;

export enum LedgerEntryDirection {
  DEBIT = 'DEBIT',
  CREDIT = 'CREDIT',
}

@Schema({ timestamps: true, collection: 'ledger_entries' })
export class LedgerEntry {
  @Prop({ type: MongooseSchema.Types.ObjectId, ref: 'Transaction', required: true })
  transactionId: Types.ObjectId;

  @Prop({ type: MongooseSchema.Types.ObjectId, ref: 'Wallet', required: true })
  walletId: Types.ObjectId;

  @Prop({ type: String, enum: LedgerEntryDirection, required: true })
  direction: LedgerEntryDirection;

  @Prop({ required: true })
  amount: number;

  @Prop({ required: true })
  balanceAfter: number;

  createdAt?: Date;
  updatedAt?: Date;
}

export const LedgerEntrySchema = SchemaFactory.createForClass(LedgerEntry);

// Supports getEntriesForWallet/computeBalanceFromLedger/aggregateNetByWallet,
// all of which filter (and the first also sorts) on walletId - without this,
// every one of them is a full collection scan.
LedgerEntrySchema.index({ walletId: 1, createdAt: 1 });

// Supports the dashboard's batched ledger lookup (`transactionId: { $in: [...] }`).
LedgerEntrySchema.index({ transactionId: 1 });
