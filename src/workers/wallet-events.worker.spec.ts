import { getModelToken } from '@nestjs/mongoose';
import { Test, TestingModule } from '@nestjs/testing';
import { Types } from 'mongoose';
import { Wallet } from '../wallets/schemas/wallet.schema';
import * as workerModule from './wallet-events.worker';
import { WalletEventsWorker } from './wallet-events.worker';

describe('WalletEventsWorker', () => {
  let worker: WalletEventsWorker;
  let walletModel: any;

  beforeEach(async () => {
    walletModel = { find: jest.fn() };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        WalletEventsWorker,
        { provide: getModelToken(Wallet.name), useValue: walletModel },
      ],
    }).compile();

    worker = module.get(WalletEventsWorker);
  });

  afterEach(() => jest.clearAllMocks());

  function mockFind(wallets: unknown[]) {
    const exec = jest.fn().mockResolvedValue(wallets);
    const lean = jest.fn().mockReturnValue({ exec });
    const limit = jest.fn().mockReturnValue({ lean });
    const sort = jest.fn().mockReturnValue({ limit });
    walletModel.find.mockReturnValue({ sort });
    return { sort, limit, lean, exec };
  }

  it('does not export an EventEmitter-based event bus (structural leak regression guard)', () => {
    expect((workerModule as any).walletEventBus).toBeUndefined();
  });

  it('queries the 20 most recently updated wallets as lean documents', async () => {
    const { sort, limit, lean } = mockFind([]);

    await (worker as any).tick();

    expect(walletModel.find).toHaveBeenCalledWith();
    expect(sort).toHaveBeenCalledWith({ updatedAt: -1 });
    expect(limit).toHaveBeenCalledWith(20);
    expect(lean).toHaveBeenCalledWith();
  });

  it('logs a balance snapshot for each returned wallet', async () => {
    const debugSpy = jest
      .spyOn((worker as any).logger, 'debug')
      .mockImplementation(() => undefined);
    const wallets = [
      { _id: new Types.ObjectId(), balance: 100 },
      { _id: new Types.ObjectId(), balance: 250 },
    ];
    mockFind(wallets);

    await (worker as any).tick();

    expect(debugSpy).toHaveBeenCalledTimes(2);
    expect(debugSpy).toHaveBeenCalledWith(
      `Wallet ${wallets[0]._id} snapshot balance=${wallets[0].balance}`,
    );
    expect(debugSpy).toHaveBeenCalledWith(
      `Wallet ${wallets[1]._id} snapshot balance=${wallets[1].balance}`,
    );
  });

  it('does not log anything when there are no recently updated wallets', async () => {
    const debugSpy = jest
      .spyOn((worker as any).logger, 'debug')
      .mockImplementation(() => undefined);
    mockFind([]);

    await (worker as any).tick();

    expect(debugSpy).not.toHaveBeenCalled();
  });

  it('isolates a tick failure instead of letting it become an unhandled rejection', async () => {
    const errorSpy = jest
      .spyOn((worker as any).logger, 'error')
      .mockImplementation(() => undefined);
    walletModel.find.mockImplementation(() => {
      throw new Error('connection reset');
    });

    await expect((worker as any).runTick()).resolves.not.toThrow();

    expect(errorSpy).toHaveBeenCalledWith(
      expect.stringContaining('connection reset'),
      expect.anything(),
    );
  });
});
