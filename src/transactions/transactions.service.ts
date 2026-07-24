import { Injectable } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { ClientSession, Model } from 'mongoose';
import {
  Transaction,
  TransactionDocument,
  TransactionStatus,
  TransactionType,
} from './schemas/transaction.schema';
import { QueryTransactionsDto } from './dto/query-transactions.dto';

export interface CreateTransactionInput {
  walletId: string;
  type: TransactionType;
  amount: number;
  balanceAfter: number;
  status?: TransactionStatus;
  reference?: string;
  transferId?: string;
  counterpartyWalletId?: string;
}

@Injectable()
export class TransactionsService {
  constructor(
    @InjectModel(Transaction.name)
    private readonly transactionModel: Model<TransactionDocument>,
  ) {}

  async create(input: CreateTransactionInput, session?: ClientSession) {
    const [transaction] = await this.transactionModel.create(
      [
        {
          walletId: input.walletId,
          type: input.type,
          amount: input.amount,
          balanceAfter: input.balanceAfter,
          status: input.status ?? TransactionStatus.COMPLETED,
          reference: input.reference,
          transferId: input.transferId,
          counterpartyWalletId: input.counterpartyWalletId,
        },
      ],
      session ? { session } : undefined,
    );
    return transaction;
  }

  async findAll(query: QueryTransactionsDto) {
    const page = query.page ?? 1;
    const limit = query.limit ?? 20;
    const filter: Record<string, unknown> = {};

    if (query.walletId) {
      filter.walletId = query.walletId;
    }
    if (query.type) {
      filter.type = query.type;
    }

    const [items, total] = await Promise.all([
      this.transactionModel
        .find(filter)
        .sort({ createdAt: -1 })
        .skip((page - 1) * limit)
        .limit(limit)
        .exec(),
      this.transactionModel.countDocuments(filter),
    ]);

    return { items, total, page, limit };
  }

  async findByReference(reference: string, session?: ClientSession) {
    const query = this.transactionModel.findOne({ reference });
    return session ? query.session(session).exec() : query.exec();
  }
}
