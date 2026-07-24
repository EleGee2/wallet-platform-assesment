import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { RabbitMQService } from '../queue/rabbitmq.service';
import { Transfer, TransferDocument, TransferStatus } from '../wallets/schemas/transfer.schema';

@Injectable()
export class PendingTransferWorker implements OnModuleInit, OnModuleDestroy {
  private static readonly SWEEP_BATCH_SIZE = 100;

  private readonly logger = new Logger(PendingTransferWorker.name);
  private timer: NodeJS.Timeout;

  constructor(
    @InjectModel(Transfer.name) private readonly transferModel: Model<TransferDocument>,
    private readonly configService: ConfigService,
    private readonly rabbitMQService: RabbitMQService,
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

    // Mark the whole batch as swept before attempting any publish, so a
    // persistently-unreachable broker also backs off to once per timeout
    // window instead of being hammered every tick.
    await this.transferModel.updateMany(
      { _id: { $in: stale.map((transfer) => transfer._id) } },
      { $set: { lastSweptAt: new Date() } },
    );

    // Re-publishing is safe by construction: the consumer's status-guarded
    // completion makes reprocessing an already-COMPLETED transfer a no-op, so
    // this self-heals the common "stuck forever" causes (message lost between
    // publish and commit, consumer wasn't running, one nack-retry wasn't
    // enough) without a terminal fail-and-refund path.
    for (const transfer of stale) {
      try {
        await this.rabbitMQService.publish(
          'transfer.initiated',
          {
            transferId: transfer.id,
            fromWalletId: transfer.fromWalletId.toString(),
            toWalletId: transfer.toWalletId.toString(),
            amount: transfer.amount,
          },
          transfer.correlationId,
        );
      } catch (error) {
        this.logger.error(
          `Failed to re-publish stale transfer ${transfer.id}: ${(error as Error).message}`,
        );
      }
    }
  }

  onModuleDestroy() {
    clearInterval(this.timer);
  }
}
