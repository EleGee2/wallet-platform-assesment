import { ApiPropertyOptional } from '@nestjs/swagger';
import { Type } from 'class-transformer';
import { IsIn, IsInt, IsOptional, Max, Min } from 'class-validator';
import { LedgerEntryDirection } from '../schemas/ledger-entry.schema';

export class QueryLedgerEntriesDto {
  @ApiPropertyOptional({ enum: LedgerEntryDirection })
  @IsOptional()
  @IsIn(Object.values(LedgerEntryDirection))
  direction?: LedgerEntryDirection;

  @ApiPropertyOptional({ default: 1 })
  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  page?: number = 1;

  @ApiPropertyOptional({ default: 20 })
  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  @Max(100)
  limit?: number = 20;
}
