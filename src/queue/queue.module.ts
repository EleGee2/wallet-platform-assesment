import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { LedgerModule } from '../ledger/ledger.module';
import { Transaction, TransactionSchema } from '../transactions/schemas/transaction.schema';
import { Transfer, TransferSchema } from '../wallets/schemas/transfer.schema';
import { Wallet, WalletSchema } from '../wallets/schemas/wallet.schema';
import { RabbitMQService } from './rabbitmq.service';
import { InboxMessage, InboxMessageSchema } from './schemas/inbox-message.schema';
import { TransferEventsConsumer } from './transfer-events.consumer';

@Module({
  imports: [
    MongooseModule.forFeature([
      { name: Transfer.name, schema: TransferSchema },
      { name: Wallet.name, schema: WalletSchema },
      { name: Transaction.name, schema: TransactionSchema },
      { name: InboxMessage.name, schema: InboxMessageSchema },
    ]),
    LedgerModule,
  ],
  providers: [RabbitMQService, TransferEventsConsumer],
  exports: [RabbitMQService],
})
export class QueueModule {}
