import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { HydratedDocument } from 'mongoose';

export type WalletDocument = HydratedDocument<Wallet>;

@Schema({ timestamps: true, collection: 'wallets' })
export class Wallet {
  @Prop({ required: true, index: true })
  userId: string;

  @Prop({ required: true, trim: true })
  ownerName: string;

  @Prop({ required: true, default: 'GHS' })
  currency: string;

  @Prop({ required: true, default: 0 })
  balance: number;

  // Intended for optimistic concurrency control on concurrent balance mutations.
  @Prop({ required: true, default: 0 })
  version: number;

  createdAt?: Date;
  updatedAt?: Date;
}

export const WalletSchema = SchemaFactory.createForClass(Wallet);

WalletSchema.index({ updatedAt: -1 });
