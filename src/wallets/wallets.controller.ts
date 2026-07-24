import { Body, Controller, Get, Param, Post, Query } from '@nestjs/common';
import { ApiBearerAuth, ApiTags } from '@nestjs/swagger';
import { Throttle } from '@nestjs/throttler';
import { QueryLedgerEntriesDto } from '../ledger/dto/query-ledger-entries.dto';
import { CreateWalletDto } from './dto/create-wallet.dto';
import { DepositDto } from './dto/deposit.dto';
import { TransferDto } from './dto/transfer.dto';
import { WithdrawDto } from './dto/withdraw.dto';
import { WalletsService } from './wallets.service';

@ApiTags('wallets')
@Controller('wallets')
@ApiBearerAuth()
export class WalletsController {
  constructor(private readonly walletsService: WalletsService) {}

  @Post()
  create(@Body() dto: CreateWalletDto) {
    return this.walletsService.createWallet(dto);
  }

  @Throttle({ default: { limit: 15, ttl: 10000 } })
  @Post('transfer')
  transfer(@Body() dto: TransferDto) {
    return this.walletsService.transfer(dto);
  }

  @Get(':id')
  findOne(@Param('id') id: string) {
    return this.walletsService.getWallet(id);
  }

  @Get(':id/dashboard')
  dashboard(@Param('id') id: string) {
    return this.walletsService.getDashboard(id);
  }

  @Get(':id/reconcile')
  reconcile(@Param('id') id: string) {
    return this.walletsService.reconcileWallet(id);
  }

  @Get(':id/audit')
  audit(@Param('id') id: string, @Query() query: QueryLedgerEntriesDto) {
    return this.walletsService.getAudit(id, query);
  }

  @Post(':id/deposit')
  deposit(@Param('id') id: string, @Body() dto: DepositDto) {
    return this.walletsService.deposit(id, dto);
  }

  @Post(':id/withdraw')
  withdraw(@Param('id') id: string, @Body() dto: WithdrawDto) {
    return this.walletsService.withdraw(id, dto);
  }
}
