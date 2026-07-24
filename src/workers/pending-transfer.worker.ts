import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { InjectConnection, InjectModel } from '@nestjs/mongoose';
import { Connection, Model } from 'mongoose';
import { requestContext } from '../common/context/request-context';
import { OutboxService } from '../outbox/outbox.service';
import { Transfer, TransferDocument, TransferStatus } from '../wallets/schemas/transfer.schema';

@Injectable()
export class PendingTransferWorker implements OnModuleInit, OnModuleDestroy {
  private static readonly SWEEP_BATCH_SIZE = 100;

  private readonly logger = new Logger(PendingTransferWorker.name);
  private timer: NodeJS.Timeout;

  constructor(
    @InjectConnection() private readonly connection: Connection,
    @InjectModel(Transfer.name) private readonly transferModel: Model<TransferDocument>,
    private readonly configService: ConfigService,
    private readonly outboxService: OutboxService,
  ) {}

  onModuleInit() {
    const intervalMs = this.configService.getOrThrow<number>(
      'workers.pendingTransferSweepIntervalMs',
    );
    this.timer = setInterval(() => this.sweep(), intervalMs);
  }

  private async sweep() {
    const timeoutMs = this.configService.getOrThrow<number>('workers.pendingTransferTimeoutMs');
    const cutoff = new Date(Date.now() - timeoutMs);

    // Only transfers that are stale AND haven't been re-swept within the same
    // timeout window - without this, a still-stuck (or poison) transfer would
    // be re-published on every tick (pendingTransferSweepIntervalMs) instead
    // of once per pendingTransferTimeoutMs, flooding the broker.
    const stale = await this.transferModel
      .find({
        status: TransferStatus.PENDING,
        createdAt: { $lt: cutoff },
        $or: [{ lastSweptAt: { $exists: false } }, { lastSweptAt: { $lt: cutoff } }],
      })
      .limit(PendingTransferWorker.SWEEP_BATCH_SIZE)
      .exec();

    if (stale.length === 0) {
      return;
    }

    this.logger.warn(
      `Found ${stale.length} transfer(s) pending past the timeout window - re-publishing`,
    );

    // Re-publishing is safe by construction: the consumer's status-guarded
    // completion makes reprocessing an already-COMPLETED transfer a no-op, so
    // this self-heals the common "stuck forever" causes (message lost between
    // publish and commit, consumer wasn't running, one nack-retry wasn't
    // enough) without a terminal fail-and-refund path.
    //
    // Each transfer's "mark swept" write and its outbox stage happen in one
    // transaction, so a crash between them can't silently lose the republish
    // until the next full timeout window - the same guarantee `createWallet`/
    // `transfer` already give their own outbox events.
    for (const transfer of stale) {
      try {
        // The sweep runs on a timer, outside any request - re-establish the
        // originating request's correlation id (stored on the transfer at
        // creation time) so OutboxService.enqueue's ambient read picks it up
        // instead of staging the event with none.
        await requestContext.run({ correlationId: transfer.correlationId }, async () => {
          const session = await this.connection.startSession();
          try {
            await session.withTransaction(async () => {
              const swept = await this.transferModel.updateOne(
                { _id: transfer._id, status: TransferStatus.PENDING },
                { $set: { lastSweptAt: new Date() } },
                { session },
              );

              if (swept.matchedCount === 0) {
                return;
              }

              await this.outboxService.enqueue(
                'transfer.initiated',
                {
                  transferId: transfer.id,
                  fromWalletId: transfer.fromWalletId.toString(),
                  toWalletId: transfer.toWalletId.toString(),
                  amount: transfer.amount,
                },
                session,
              );
            });
          } finally {
            await session.endSession();
          }
        });
      } catch (error) {
        this.logger.error(
          `Failed to stage republish for stale transfer ${transfer.id}: ${(error as Error).message}`,
        );
      }
    }
  }

  onModuleDestroy() {
    clearInterval(this.timer);
  }
}
