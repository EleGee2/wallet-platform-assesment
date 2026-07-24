import { getModelToken } from '@nestjs/mongoose';
import { Test, TestingModule } from '@nestjs/testing';
import { Transaction, TransactionType } from './schemas/transaction.schema';
import { TransactionsService } from './transactions.service';

describe('TransactionsService', () => {
  let service: TransactionsService;
  let transactionModel: any;

  beforeEach(async () => {
    transactionModel = {
      create: jest.fn(),
      find: jest.fn(),
      countDocuments: jest.fn(),
      findOne: jest.fn(),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        TransactionsService,
        { provide: getModelToken(Transaction.name), useValue: transactionModel },
      ],
    }).compile();

    service = module.get(TransactionsService);
  });

  it('creates a transaction document', async () => {
    const created = { _id: '1', type: TransactionType.DEPOSIT };
    transactionModel.create.mockResolvedValue([created]);

    const result = await service.create({
      walletId: 'wallet-1',
      type: TransactionType.DEPOSIT,
      amount: 100,
      balanceAfter: 100,
    });

    expect(transactionModel.create).toHaveBeenCalledWith(
      [expect.objectContaining({ walletId: 'wallet-1', amount: 100 })],
      undefined,
    );
    expect(result).toBe(created);
  });

  it('paginates results and returns a total count', async () => {
    const items = [{ id: '1' }, { id: '2' }];
    const execMock = jest.fn().mockResolvedValue(items);
    transactionModel.find.mockReturnValue({
      sort: jest.fn().mockReturnThis(),
      skip: jest.fn().mockReturnThis(),
      limit: jest.fn().mockReturnThis(),
      exec: execMock,
    });
    transactionModel.countDocuments.mockResolvedValue(42);

    const result = await service.findAll({ page: 2, limit: 2 });

    expect(result).toEqual({ items, total: 42, page: 2, limit: 2 });
  });

  it('filters by type alone, with no walletId, across all wallets', async () => {
    const execMock = jest.fn().mockResolvedValue([]);
    transactionModel.find.mockReturnValue({
      sort: jest.fn().mockReturnThis(),
      skip: jest.fn().mockReturnThis(),
      limit: jest.fn().mockReturnThis(),
      exec: execMock,
    });
    transactionModel.countDocuments.mockResolvedValue(0);

    await service.findAll({ type: TransactionType.WITHDRAWAL, page: 1, limit: 20 });

    expect(transactionModel.find).toHaveBeenCalledWith({ type: TransactionType.WITHDRAWAL });
    expect(transactionModel.countDocuments).toHaveBeenCalledWith({
      type: TransactionType.WITHDRAWAL,
    });
  });

  it('combines walletId and type filters when both are supplied', async () => {
    const execMock = jest.fn().mockResolvedValue([]);
    transactionModel.find.mockReturnValue({
      sort: jest.fn().mockReturnThis(),
      skip: jest.fn().mockReturnThis(),
      limit: jest.fn().mockReturnThis(),
      exec: execMock,
    });
    transactionModel.countDocuments.mockResolvedValue(0);

    await service.findAll({
      walletId: 'wallet-1',
      type: TransactionType.DEPOSIT,
      page: 1,
      limit: 20,
    });

    expect(transactionModel.find).toHaveBeenCalledWith({
      walletId: 'wallet-1',
      type: TransactionType.DEPOSIT,
    });
  });
});
