import {
  BadRequestException,
  ConflictException,
  Injectable,
  NotFoundException,
} from '@nestjs/common';
import { InjectConnection, InjectModel } from '@nestjs/mongoose';
import { Connection, Model, Types } from 'mongoose';
import { getCorrelationId } from '../common/context/request-context';
import { LedgerEntry, LedgerEntryDocument } from '../ledger/schemas/ledger-entry.schema';
import { LedgerService } from '../ledger/ledger.service';
import { OutboxService } from '../outbox/outbox.service';
import { RedisService } from '../redis/redis.service';
import { TransactionsService } from '../transactions/transactions.service';
import {
  Transaction,
  TransactionDocument,
  TransactionStatus,
  TransactionType,
} from '../transactions/schemas/transaction.schema';
import { CreateWalletDto } from './dto/create-wallet.dto';
import { DepositDto } from './dto/deposit.dto';
import { TransferDto } from './dto/transfer.dto';
import { WithdrawDto } from './dto/withdraw.dto';
import { Transfer, TransferDocument, TransferStatus } from './schemas/transfer.schema';
import { Wallet, WalletDocument } from './schemas/wallet.schema';

// Internal signal from transfer()'s transaction callback to its outer catch that
// this attempt lost an idempotency-key race against a concurrent request - distinct
// from any other error the callback can throw, so it's never mistaken for one.
class IdempotentReplayError extends Error {}

@Injectable()
export class WalletsService {
  constructor(
    @InjectConnection() private readonly connection: Connection,
    @InjectModel(Wallet.name) private readonly walletModel: Model<WalletDocument>,
    @InjectModel(Transfer.name) private readonly transferModel: Model<TransferDocument>,
    @InjectModel(Transaction.name) private readonly transactionModel: Model<TransactionDocument>,
    @InjectModel(LedgerEntry.name) private readonly ledgerEntryModel: Model<LedgerEntryDocument>,
    private readonly transactionsService: TransactionsService,
    private readonly ledgerService: LedgerService,
    private readonly outboxService: OutboxService,
    private readonly redisService: RedisService,
  ) {}

  async createWallet(dto: CreateWalletDto) {
    const session = await this.connection.startSession();
    let wallet!: WalletDocument;

    try {
      await session.withTransaction(async () => {
        [wallet] = await this.walletModel.create(
          [
            {
              userId: dto.userId,
              ownerName: dto.ownerName,
              currency: dto.currency ?? 'GHS',
              balance: 0,
            },
          ],
          { session },
        );

        await this.outboxService.enqueue(
          'wallet.created',
          {
            walletId: wallet._id.toString(),
            userId: wallet.userId,
            currency: wallet.currency,
          },
          session,
        );
      });
    } finally {
      await session.endSession();
    }

    return wallet;
  }

  async getWallet(id: string) {
    const wallet = await this.walletModel.findById(id);
    if (!wallet) {
      throw new NotFoundException(`Wallet ${id} not found`);
    }

    const cachedBalance = await this.redisService.getCachedBalance(id);
    if (cachedBalance !== null) {
      return { ...wallet.toObject(), balance: cachedBalance };
    }

    await this.redisService.setCachedBalance(id, wallet.balance);
    return wallet;
  }

  async deposit(id: string, dto: DepositDto) {
    const session = await this.connection.startSession();
    let wallet!: WalletDocument;

    try {
      await session.withTransaction(async () => {
        // Idempotency pre-check: fast path for a client retrying with a reference
        // that already completed. The unique+sparse index on Transaction.reference
        // is the real concurrency backstop (see catch below) - this is just the
        // common case, so a retry doesn't even attempt a second balance mutation.
        if (dto.reference) {
          const existing = await this.transactionsService.findByReference(dto.reference, session);
          if (existing) {
            throw new ConflictException(
              `Deposit with reference ${dto.reference} was already processed`,
            );
          }
        }

        const updated = await this.walletModel.findByIdAndUpdate(
          id,
          { $inc: { balance: dto.amount } },
          { new: true, session },
        );

        if (!updated) {
          throw new NotFoundException(`Wallet ${id} not found`);
        }

        wallet = updated;

        let transaction: TransactionDocument;
        try {
          transaction = await this.transactionsService.create(
            {
              walletId: wallet.id,
              type: TransactionType.DEPOSIT,
              amount: dto.amount,
              balanceAfter: wallet.balance,
              reference: dto.reference,
            },
            session,
          );
        } catch (error) {
          // A concurrent request racing on the same reference can slip past the
          // pre-check above and only collide here, at the unique index. Converting
          // it to a 409 (instead of a raw Mongo error) also rolls back the balance
          // increment above, since it's all one transaction.
          if ((error as { code?: number }).code === 11000) {
            throw new ConflictException(
              `Deposit with reference ${dto.reference} was already processed`,
            );
          }
          throw error;
        }

        await this.ledgerService.recordCredit(
          wallet._id,
          transaction._id,
          dto.amount,
          wallet.balance,
          session,
        );
      });
    } finally {
      await session.endSession();
    }

    // Outside the transaction: withTransaction retries its whole callback on
    // a transient write conflict, so invalidating inside it could fire before
    // the eventual commit and leave a window for a concurrent read to
    // re-populate the cache with a value that's about to go stale again.
    await this.redisService.invalidateBalance(id);

    return wallet;
  }

  async withdraw(id: string, dto: WithdrawDto) {
    const session = await this.connection.startSession();
    let wallet!: WalletDocument;

    try {
      await session.withTransaction(async () => {
        // Idempotency pre-check: fast path for a client retrying with a reference
        // that already completed. The unique+sparse index on Transaction.reference
        // is the real concurrency backstop (see catch below) - this is just the
        // common case, so a retry doesn't even attempt a second balance mutation.
        if (dto.reference) {
          const existing = await this.transactionsService.findByReference(dto.reference, session);
          if (existing) {
            throw new ConflictException(
              `Withdrawal with reference ${dto.reference} was already processed`,
            );
          }
        }

        // Atomic, conditional decrement: the balance check and the mutation happen
        // as a single command, so two concurrent withdrawals can't both read the
        // same starting balance and both succeed against it.
        const updated = await this.walletModel.findOneAndUpdate(
          { _id: id, balance: { $gte: dto.amount } },
          { $inc: { balance: -dto.amount } },
          { new: true, session },
        );

        if (!updated) {
          const walletExists = await this.walletModel.exists({ _id: id });
          if (!walletExists) {
            throw new NotFoundException(`Wallet ${id} not found`);
          }
          throw new BadRequestException('Insufficient balance');
        }

        wallet = updated;

        let transaction: TransactionDocument;
        try {
          transaction = await this.transactionsService.create(
            {
              walletId: wallet.id,
              type: TransactionType.WITHDRAWAL,
              amount: dto.amount,
              balanceAfter: wallet.balance,
              reference: dto.reference,
            },
            session,
          );
        } catch (error) {
          // A concurrent request racing on the same reference can slip past the
          // pre-check above and only collide here, at the unique index. Converting
          // it to a 409 (instead of a raw Mongo error) also rolls back the balance
          // decrement above, since it's all one transaction.
          if ((error as { code?: number }).code === 11000) {
            throw new ConflictException(
              `Withdrawal with reference ${dto.reference} was already processed`,
            );
          }
          throw error;
        }

        await this.ledgerService.recordDebit(
          wallet._id,
          transaction._id,
          dto.amount,
          wallet.balance,
          session,
        );
      });
    } finally {
      await session.endSession();
    }

    // Outside the transaction: withTransaction retries its whole callback on
    // a transient write conflict, so invalidating inside it could fire before
    // the eventual commit and leave a window for a concurrent read to
    // re-populate the cache with a value that's about to go stale again.
    await this.redisService.invalidateBalance(wallet.id);

    return wallet;
  }

  async transfer(dto: TransferDto) {
    if (dto.fromWalletId === dto.toWalletId) {
      throw new BadRequestException('Cannot transfer to the same wallet');
    }

    // Idempotency pre-check: fast path for a client retrying with a key that
    // already produced a transfer. Runs before any wallet reads, so a pure
    // sequential replay costs one indexed lookup and no new debit - matches
    // the DTO's own contract ("retried requests should reuse the same key"),
    // so the replay returns the original result rather than an error.
    if (dto.idempotencyKey) {
      const existing = await this.transferModel.findOne({ idempotencyKey: dto.idempotencyKey });
      if (existing) {
        return existing;
      }
    }

    const [fromWallet, toWallet] = await Promise.all([
      this.walletModel.findById(dto.fromWalletId),
      this.walletModel.findById(dto.toWalletId),
    ]);

    if (!fromWallet || !toWallet) {
      throw new NotFoundException('Wallet not found');
    }

    const session = await this.connection.startSession();
    let transfer!: TransferDocument;

    try {
      try {
        await session.withTransaction(async () => {
          // Atomic, conditional debit - same guard as withdraw(). Existence was
          // already confirmed above, so a null result here can only mean
          // insufficient balance.
          const updatedFromWallet = await this.walletModel.findOneAndUpdate(
            { _id: fromWallet._id, balance: { $gte: dto.amount } },
            { $inc: { balance: -dto.amount } },
            { new: true, session },
          );

          if (!updatedFromWallet) {
            throw new BadRequestException('Insufficient balance');
          }

          let createdTransfer: TransferDocument;
          try {
            [createdTransfer] = await this.transferModel.create(
              [
                {
                  fromWalletId: fromWallet._id,
                  toWalletId: toWallet._id,
                  amount: dto.amount,
                  status: TransferStatus.PENDING,
                  idempotencyKey: dto.idempotencyKey,
                  correlationId: getCorrelationId(),
                },
              ],
              { session },
            );
          } catch (error) {
            if ((error as { code?: number }).code === 11000) {
              // Lost a race against another in-flight request with the same
              // idempotencyKey (both passed the pre-check before either had
              // committed). Abort - the $inc above rolls back with it, since
              // it's all one transaction - and let the outer catch return the
              // winner's transfer instead of creating a duplicate debit.
              throw new IdempotentReplayError();
            }
            throw error;
          }
          transfer = createdTransfer;

          const [debitTransaction] = await this.transactionModel.create(
            [
              {
                walletId: updatedFromWallet._id,
                type: TransactionType.TRANSFER_OUT,
                amount: dto.amount,
                status: TransactionStatus.COMPLETED,
                balanceAfter: updatedFromWallet.balance,
                transferId: transfer._id,
                counterpartyWalletId: toWallet._id,
              },
            ],
            { session },
          );

          await this.ledgerService.recordDebit(
            updatedFromWallet._id,
            debitTransaction._id,
            dto.amount,
            updatedFromWallet.balance,
            session,
          );

          // Staged in the same transaction rather than published directly:
          // session.withTransaction retries this whole callback on a transient
          // write conflict, and a direct RabbitMQService.publish() call would
          // fire for real on both the aborted attempt and the retry. An outbox
          // write is just another Mongo write, so it lives or dies with the
          // rest of the transaction instead.
          await this.outboxService.enqueue(
            'transfer.initiated',
            {
              transferId: transfer._id.toString(),
              fromWalletId: updatedFromWallet._id.toString(),
              toWalletId: toWallet._id.toString(),
              amount: dto.amount,
            },
            session,
          );
        });
      } catch (error) {
        if (error instanceof IdempotentReplayError) {
          const existing = await this.transferModel.findOne({
            idempotencyKey: dto.idempotencyKey,
          });
          if (!existing) {
            throw error;
          }
          transfer = existing;
        } else {
          throw error;
        }
      }
    } finally {
      await session.endSession();
    }

    // Covers both a fresh debit and the duplicate-idempotency-key-race path
    // (where another request's write already invalidated this same key -
    // redundant here, but invalidation is idempotent, so harmless).
    await this.redisService.invalidateBalance(dto.fromWalletId);

    return transfer;
  }

  async getDashboard(id: string) {
    const wallet = await this.walletModel.findById(id).lean().exec();
    if (!wallet) {
      throw new NotFoundException(`Wallet ${id} not found`);
    }

    // Totals/count computed in the database over the wallet's full history,
    // instead of looping every transaction the wallet has ever made in Node.
    // `.aggregate()` doesn't cast query values like `.find()` does, so the
    // walletId has to be cast explicitly - see LedgerService.aggregateNetByWallet
    // for the same requirement elsewhere in this codebase.
    const [stats] = await this.transactionModel.aggregate([
      { $match: { walletId: new Types.ObjectId(id) } },
      {
        $group: {
          _id: null,
          transactionCount: { $sum: 1 },
          totalDeposited: {
            $sum: {
              $cond: [
                { $in: ['$type', [TransactionType.DEPOSIT, TransactionType.TRANSFER_IN]] },
                '$amount',
                0,
              ],
            },
          },
          totalWithdrawn: {
            $sum: {
              $cond: [
                { $in: ['$type', [TransactionType.DEPOSIT, TransactionType.TRANSFER_IN]] },
                0,
                '$amount',
              ],
            },
          },
        },
      },
    ]);

    // Only the transactions actually shown, not the wallet's whole history.
    const recentTransactions = await this.transactionModel
      .find({ walletId: id })
      .sort({ createdAt: -1 })
      .limit(10)
      .lean()
      .exec();

    // One batched query for their ledger entries, instead of one per transaction.
    const recentTransactionIds = recentTransactions.map((txn) => txn._id);
    const ledgerEntries = await this.ledgerEntryModel
      .find({ transactionId: { $in: recentTransactionIds } })
      .lean()
      .exec();

    const entriesByTransactionId = new Map<string, typeof ledgerEntries>();
    for (const entry of ledgerEntries) {
      const key = entry.transactionId.toString();
      const existing = entriesByTransactionId.get(key);
      if (existing) {
        existing.push(entry);
      } else {
        entriesByTransactionId.set(key, [entry]);
      }
    }

    const recentActivity = recentTransactions.map((transaction) => ({
      transaction,
      entries: entriesByTransactionId.get(transaction._id.toString()) ?? [],
    }));

    return {
      wallet,
      totalDeposited: stats?.totalDeposited ?? 0,
      totalWithdrawn: stats?.totalWithdrawn ?? 0,
      transactionCount: stats?.transactionCount ?? 0,
      recentActivity,
    };
  }
}
