import { CallHandler, ExecutionContext, Logger } from '@nestjs/common';
import { lastValueFrom, of } from 'rxjs';
import { requestContext } from '../context/request-context';
import { LoggingInterceptor } from './logging.interceptor';

describe('LoggingInterceptor', () => {
  let interceptor: LoggingInterceptor;
  let logSpy: jest.SpyInstance;

  beforeEach(() => {
    interceptor = new LoggingInterceptor();
    logSpy = jest.spyOn(Logger.prototype, 'log').mockImplementation(() => undefined);
  });

  afterEach(() => jest.restoreAllMocks());

  function mockContext(method: string, url: string): ExecutionContext {
    return {
      switchToHttp: () => ({
        getRequest: () => ({ method, url }),
      }),
    } as unknown as ExecutionContext;
  }

  function mockHandler(): CallHandler {
    return { handle: () => of('result') };
  }

  it('includes the ambient correlation id in the log line', async () => {
    await requestContext.run({ correlationId: 'corr-abc' }, async () => {
      await lastValueFrom(interceptor.intercept(mockContext('GET', '/wallets/1'), mockHandler()));
    });

    expect(logSpy).toHaveBeenCalledWith(expect.stringContaining('[corr-abc] GET /wallets/1'));
  });

  it('logs a placeholder instead of crashing when there is no correlation id', async () => {
    await lastValueFrom(interceptor.intercept(mockContext('POST', '/wallets'), mockHandler()));

    expect(logSpy).toHaveBeenCalledWith(expect.stringContaining('[-] POST /wallets'));
  });
});
