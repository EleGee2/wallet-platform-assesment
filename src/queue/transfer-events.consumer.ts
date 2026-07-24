import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { InjectConnection, InjectModel } from '@nestjs/mongoose';
import type { Channel, ConfirmChannel, ConsumeMessage } from 'amqplib';
import { Connection, Model } from 'mongoose';
import { getCorrelationId, requestContext } from '../common/context/request-context';
import { LedgerService } from '../ledger/ledger.service';
import { RedisService } from '../redis/redis.service';
import {
  Transaction,
  TransactionDocument,
  TransactionStatus,
  TransactionType,
} from '../transactions/schemas/transaction.schema';
import { Transfer, TransferDocument, TransferStatus } from '../wallets/schemas/transfer.schema';
import { Wallet, WalletDocument } from '../wallets/schemas/wallet.schema';
import { RabbitMQService } from './rabbitmq.service';
import { InboxMessage, InboxMessageDocument } from './schemas/inbox-message.schema';

export interface TransferInitiatedEvent {
  transferId: string;
  fromWalletId: string;
  toWalletId: string;
  amount: number;
}

@Injectable()
export class TransferEventsConsumer implements OnModuleInit {
  private readonly logger = new Logger(TransferEventsConsumer.name);

  constructor(
    private readonly rabbitMQService: RabbitMQService,
    @InjectConnection() private readonly connection: Connection,
    @InjectModel(Transfer.name) private readonly transferModel: Model<TransferDocument>,
    @InjectModel(Wallet.name) private readonly walletModel: Model<WalletDocument>,
    @InjectModel(Transaction.name)
    private readonly transactionModel: Model<TransactionDocument>,
    @InjectModel(InboxMessage.name)
    private readonly inboxMessageModel: Model<InboxMessageDocument>,
    private readonly ledgerService: LedgerService,
    private readonly redisService: RedisService,
  ) {}

  onModuleInit() {
    const channelWrapper = this.rabbitMQService.getChannelWrapper();
    const queue = this.rabbitMQService.getTransferQueue();

    channelWrapper.addSetup((channel: ConfirmChannel) =>
      channel.consume(queue, (message) => this.handleMessage(message, channel)),
    );
  }

  private async handleMessage(message: ConsumeMessage | null, channel: Channel) {
    if (!message) {
      return;
    }

    // Re-establishes the originating HTTP request's context on the consuming
    // side (the message carries it as a real AMQP correlationId property, set
    // by whichever publish path produced it), so every log line during
    // processing traces back to it.
    const correlationId = message.properties?.correlationId as string | undefined;
    const messageId = message.properties?.messageId as string | undefined;

    await requestContext.run({ correlationId }, async () => {
      let event: TransferInitiatedEvent | undefined;
      try {
        event = JSON.parse(message.content.toString());
        await this.completeTransfer(event!, messageId);
        channel.ack(message);
      } catch (error) {
        const alreadyRetried = message.fields.redelivered;
        this.logger.error(
          `[${getCorrelationId() ?? '-'}] Failed to process transfer event` +
            `${event ? ` for transfer ${event.transferId}` : ''} ` +
            `(redelivered=${alreadyRetried}): ${(error as Error).message}`,
        );

        // Bound the blast radius of a transient or poison message to one
        // redelivery, using RabbitMQ's own `redelivered` flag, instead of the
        // previous behavior of unconditionally acking (silently dropping) it.
        // A message that exhausts this retry is dead-lettered (RabbitMQService's
        // queue topology), not lost - see DESIGN.md.
        if (alreadyRetried) {
          this.logger.error(
            `[${getCorrelationId() ?? '-'}] Dropping transfer event` +
              `${event ? ` for transfer ${event.transferId}` : ''} ` +
              'after exhausting its one retry - dead-lettered for investigation',
          );
          channel.nack(message, false, false);
        } else {
          channel.nack(message, false, true);
        }
      }
    });
  }

  private async completeTransfer(event: TransferInitiatedEvent, messageId?: string) {
    // Fast pre-check: skips a wasted transaction attempt on the common
    // repeat-delivery case. Not the real guarantee (the claim inside the
    // transaction below is) - just avoids doing the work twice.
    if (messageId && (await this.inboxMessageModel.exists({ messageId }))) {
      this.logger.warn(
        `[${getCorrelationId() ?? '-'}] Message ${messageId} already processed, skipping`,
      );
      return;
    }

    const session = await this.connection.startSession();

    try {
      await session.withTransaction(async () => {
        if (messageId) {
          // Claimed first, inside the transaction: a genuinely failed
          // attempt further down (e.g. destination wallet not found) rolls
          // this back together with everything else, so a legitimate
          // redelivery retry still gets a fair attempt instead of being
          // permanently "poisoned" by a failed claim.
          try {
            await this.inboxMessageModel.create([{ messageId }], { session });
          } catch (error) {
            if ((error as { code?: number }).code === 11000) {
              this.logger.warn(
                `[${getCorrelationId() ?? '-'}] Message ${messageId} already processed ` +
                  '(lost the claim race), skipping',
              );
              return;
            }
            throw error;
          }
        }

        // Atomic, status-guarded claim: only one execution of this - across
        // redeliveries, duplicate messages, or the sweep worker's republish -
        // will ever see status flip PENDING -> COMPLETED. A null result means
        // "already handled or doesn't exist", a safe no-op either way.
        const transfer = await this.transferModel.findOneAndUpdate(
          { _id: event.transferId, status: TransferStatus.PENDING },
          { status: TransferStatus.COMPLETED },
          { new: true, session },
        );

        if (!transfer) {
          this.logger.warn(
            `[${getCorrelationId() ?? '-'}] Transfer ${event.transferId} not pending, ` +
              'skipping (already processed or missing)',
          );
          return;
        }

        const toWallet = await this.walletModel.findOneAndUpdate(
          { _id: event.toWalletId },
          { $inc: { balance: event.amount } },
          { new: true, session },
        );

        if (!toWallet) {
          // Structurally shouldn't happen (wallets aren't deleted), but if it
          // does, abort so the status flip above rolls back too - the transfer
          // stays honestly PENDING for a retry to pick up, instead of COMPLETED
          // with no matching credit.
          throw new Error(`Destination wallet ${event.toWalletId} not found`);
        }

        const [creditTransaction] = await this.transactionModel.create(
          [
            {
              walletId: toWallet._id,
              type: TransactionType.TRANSFER_IN,
              amount: event.amount,
              status: TransactionStatus.COMPLETED,
              balanceAfter: toWallet.balance,
              transferId: transfer._id,
              counterpartyWalletId: transfer.fromWalletId,
            },
          ],
          { session },
        );

        await this.ledgerService.recordCredit(
          toWallet._id,
          creditTransaction._id,
          event.amount,
          toWallet.balance,
          session,
        );

        this.logger.log(
          `[${getCorrelationId() ?? '-'}] Transfer ${transfer.id} completed for wallet ${toWallet.id}`,
        );
      });
    } finally {
      await session.endSession();
    }

    // Unconditional and outside the transaction: safe even on a no-op
    // redelivery (the original successful delivery already invalidated this
    // key - a redundant invalidation here is harmless).
    await this.redisService.invalidateBalance(event.toWalletId);
  }
}
