import { getConnectionToken, getModelToken } from '@nestjs/mongoose';
import { Test, TestingModule } from '@nestjs/testing';
import { Types } from 'mongoose';
import { LedgerService } from '../ledger/ledger.service';
import { RedisService } from '../redis/redis.service';
import { Transaction, TransactionType } from '../transactions/schemas/transaction.schema';
import { Transfer, TransferStatus } from '../wallets/schemas/transfer.schema';
import { Wallet } from '../wallets/schemas/wallet.schema';
import { RabbitMQService } from './rabbitmq.service';
import { TransferEventsConsumer } from './transfer-events.consumer';

describe('TransferEventsConsumer', () => {
  let consumer: TransferEventsConsumer;
  let transferModel: any;
  let walletModel: any;
  let transactionModel: any;
  let ledgerService: any;
  let redisService: any;

  const mockSession = {
    withTransaction: jest.fn(async (fn: () => Promise<unknown>) => fn()),
    endSession: jest.fn(),
  };

  beforeEach(async () => {
    transferModel = { findOneAndUpdate: jest.fn() };
    walletModel = { findOneAndUpdate: jest.fn() };
    transactionModel = { create: jest.fn() };
    ledgerService = { recordCredit: jest.fn() };
    redisService = { invalidateBalance: jest.fn() };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        TransferEventsConsumer,
        {
          provide: getConnectionToken(),
          useValue: { startSession: jest.fn().mockResolvedValue(mockSession) },
        },
        {
          provide: RabbitMQService,
          useValue: { getChannelWrapper: jest.fn(), getTransferQueue: jest.fn() },
        },
        { provide: getModelToken(Transfer.name), useValue: transferModel },
        { provide: getModelToken(Wallet.name), useValue: walletModel },
        { provide: getModelToken(Transaction.name), useValue: transactionModel },
        { provide: LedgerService, useValue: ledgerService },
        { provide: RedisService, useValue: redisService },
      ],
    }).compile();

    consumer = module.get(TransferEventsConsumer);
  });

  afterEach(() => jest.clearAllMocks());

  describe('completeTransfer', () => {
    it('atomically claims the transfer, credits the destination wallet, and records the ledger entry', async () => {
      const transferId = new Types.ObjectId();
      const claimedTransfer = {
        _id: transferId,
        id: transferId.toString(),
        status: TransferStatus.COMPLETED,
        fromWalletId: 'wallet-1',
      };
      const updatedToWallet = { _id: new Types.ObjectId(), id: 'wallet-2', balance: 125 };
      transferModel.findOneAndUpdate.mockResolvedValue(claimedTransfer);
      walletModel.findOneAndUpdate.mockResolvedValue(updatedToWallet);
      const creditTransaction = { _id: new Types.ObjectId() };
      transactionModel.create.mockResolvedValue([creditTransaction]);

      await (consumer as any).completeTransfer({
        transferId: transferId.toString(),
        fromWalletId: 'wallet-1',
        toWalletId: updatedToWallet._id.toString(),
        amount: 25,
      });

      expect(transferModel.findOneAndUpdate).toHaveBeenCalledWith(
        { _id: transferId.toString(), status: TransferStatus.PENDING },
        { status: TransferStatus.COMPLETED },
        { new: true, session: mockSession },
      );
      expect(walletModel.findOneAndUpdate).toHaveBeenCalledWith(
        { _id: updatedToWallet._id.toString() },
        { $inc: { balance: 25 } },
        { new: true, session: mockSession },
      );
      expect(transactionModel.create).toHaveBeenCalledWith(
        [
          expect.objectContaining({
            type: TransactionType.TRANSFER_IN,
            amount: 25,
            balanceAfter: 125,
          }),
        ],
        { session: mockSession },
      );
      expect(ledgerService.recordCredit).toHaveBeenCalledWith(
        updatedToWallet._id,
        creditTransaction._id,
        25,
        125,
        mockSession,
      );
      expect(redisService.invalidateBalance).toHaveBeenCalledWith(updatedToWallet._id.toString());
    });

    it('is a no-op when the transfer no longer exists, but still invalidates the cache defensively', async () => {
      transferModel.findOneAndUpdate.mockResolvedValue(null);

      await (consumer as any).completeTransfer({
        transferId: 'missing',
        fromWalletId: 'wallet-1',
        toWalletId: 'wallet-2',
        amount: 25,
      });

      expect(walletModel.findOneAndUpdate).not.toHaveBeenCalled();
      expect(redisService.invalidateBalance).toHaveBeenCalledWith('wallet-2');
    });

    it('does not double-credit when the same event is processed twice (redelivery)', async () => {
      const transferId = new Types.ObjectId();
      const claimedTransfer = {
        _id: transferId,
        id: transferId.toString(),
        status: TransferStatus.COMPLETED,
        fromWalletId: 'wallet-1',
      };
      transferModel.findOneAndUpdate
        .mockResolvedValueOnce(claimedTransfer)
        .mockResolvedValueOnce(null);
      walletModel.findOneAndUpdate.mockResolvedValue({
        _id: new Types.ObjectId(),
        id: 'wallet-2',
        balance: 125,
      });
      transactionModel.create.mockResolvedValue([{ _id: new Types.ObjectId() }]);

      const event = {
        transferId: transferId.toString(),
        fromWalletId: 'wallet-1',
        toWalletId: 'wallet-2',
        amount: 25,
      };

      await (consumer as any).completeTransfer(event);
      await (consumer as any).completeTransfer(event);

      expect(walletModel.findOneAndUpdate).toHaveBeenCalledTimes(1);
      expect(redisService.invalidateBalance).toHaveBeenCalledTimes(2);
    });

    it('aborts (leaving the transfer PENDING) when the destination wallet is missing', async () => {
      const transferId = new Types.ObjectId();
      transferModel.findOneAndUpdate.mockResolvedValue({
        _id: transferId,
        id: transferId.toString(),
        status: TransferStatus.COMPLETED,
      });
      walletModel.findOneAndUpdate.mockResolvedValue(null);

      await expect(
        (consumer as any).completeTransfer({
          transferId: transferId.toString(),
          fromWalletId: 'wallet-1',
          toWalletId: 'missing-wallet',
          amount: 25,
        }),
      ).rejects.toThrow('Destination wallet missing-wallet not found');

      expect(transactionModel.create).not.toHaveBeenCalled();
      expect(redisService.invalidateBalance).not.toHaveBeenCalled();
    });
  });

  describe('handleMessage', () => {
    function fakeMessage(
      redelivered: boolean,
      event: Record<string, unknown>,
      properties: Record<string, unknown> = {},
    ) {
      return {
        content: Buffer.from(JSON.stringify(event)),
        fields: { redelivered },
        properties,
      } as any;
    }

    it('acks the message on success', async () => {
      const transferId = new Types.ObjectId();
      transferModel.findOneAndUpdate.mockResolvedValue({
        _id: transferId,
        id: transferId.toString(),
        status: TransferStatus.COMPLETED,
      });
      walletModel.findOneAndUpdate.mockResolvedValue({
        _id: new Types.ObjectId(),
        id: 'wallet-2',
        balance: 125,
      });
      transactionModel.create.mockResolvedValue([{ _id: new Types.ObjectId() }]);
      const channel = { ack: jest.fn(), nack: jest.fn() };
      const message = fakeMessage(false, {
        transferId: transferId.toString(),
        fromWalletId: 'wallet-1',
        toWalletId: 'wallet-2',
        amount: 25,
      });

      await (consumer as any).handleMessage(message, channel);

      expect(channel.ack).toHaveBeenCalledWith(message);
      expect(channel.nack).not.toHaveBeenCalled();
    });

    it("carries the message's correlation id into the completed-transfer log line", async () => {
      const logSpy = jest.spyOn((consumer as any).logger, 'log');
      const transferId = new Types.ObjectId();
      transferModel.findOneAndUpdate.mockResolvedValue({
        _id: transferId,
        id: transferId.toString(),
        status: TransferStatus.COMPLETED,
      });
      walletModel.findOneAndUpdate.mockResolvedValue({
        _id: new Types.ObjectId(),
        id: 'wallet-2',
        balance: 125,
      });
      transactionModel.create.mockResolvedValue([{ _id: new Types.ObjectId() }]);
      const channel = { ack: jest.fn(), nack: jest.fn() };
      const message = fakeMessage(
        false,
        {
          transferId: transferId.toString(),
          fromWalletId: 'wallet-1',
          toWalletId: 'wallet-2',
          amount: 25,
        },
        { correlationId: 'corr-msg-1' },
      );

      await (consumer as any).handleMessage(message, channel);

      expect(logSpy).toHaveBeenCalledWith(expect.stringContaining('[corr-msg-1]'));
    });

    it('nacks with requeue on a first failure', async () => {
      transferModel.findOneAndUpdate.mockRejectedValue(new Error('transient DB error'));
      const channel = { ack: jest.fn(), nack: jest.fn() };
      const message = fakeMessage(false, {
        transferId: new Types.ObjectId().toString(),
        fromWalletId: 'wallet-1',
        toWalletId: 'wallet-2',
        amount: 25,
      });

      await (consumer as any).handleMessage(message, channel);

      expect(channel.ack).not.toHaveBeenCalled();
      expect(channel.nack).toHaveBeenCalledWith(message, false, true);
    });

    it('nacks without requeue (drops) on a second consecutive failure', async () => {
      transferModel.findOneAndUpdate.mockRejectedValue(new Error('transient DB error'));
      const channel = { ack: jest.fn(), nack: jest.fn() };
      const message = fakeMessage(true, {
        transferId: new Types.ObjectId().toString(),
        fromWalletId: 'wallet-1',
        toWalletId: 'wallet-2',
        amount: 25,
      });

      await (consumer as any).handleMessage(message, channel);

      expect(channel.ack).not.toHaveBeenCalled();
      expect(channel.nack).toHaveBeenCalledWith(message, false, false);
    });
  });
});
