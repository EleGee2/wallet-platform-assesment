import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { Wallet, WalletDocument } from '../wallets/schemas/wallet.schema';

/**
 * Watches wallets whose balance recently changed and logs a snapshot for
 * downstream monitoring dashboards. Ticks on a fixed interval.
 */
@Injectable()
export class WalletEventsWorker implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(WalletEventsWorker.name);
  private timer: NodeJS.Timeout;

  constructor(@InjectModel(Wallet.name) private readonly walletModel: Model<WalletDocument>) {}

  onModuleInit() {
    this.timer = setInterval(() => this.runTick(), 10_000);
  }

  // Split from onModuleInit so a tick failure can't become an unhandled
  // promise rejection (setInterval doesn't await its callback), and so tests
  // can await this directly instead of driving a real timer.
  private runTick() {
    return this.tick().catch((error: Error) =>
      this.logger.error(`Error during wallet snapshot tick: ${error.message}`, error.stack),
    );
  }

  private async tick() {
    const recentWallets = await this.walletModel
      .find()
      .sort({ updatedAt: -1 })
      .limit(20)
      .lean()
      .exec();

    for (const wallet of recentWallets) {
      // .lean() documents don't carry Mongoose's virtual `id` getter - use
      // `_id` directly, not `.id` (which would silently log `undefined`).
      this.logger.debug(`Wallet ${wallet._id} snapshot balance=${wallet.balance}`);
    }
  }

  onModuleDestroy() {
    clearInterval(this.timer);
  }
}
