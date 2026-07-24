import { ArgumentsHost, BadRequestException, Logger } from '@nestjs/common';
import { requestContext } from '../context/request-context';
import { AllExceptionsFilter } from './all-exceptions.filter';

describe('AllExceptionsFilter', () => {
  let filter: AllExceptionsFilter;
  let errorSpy: jest.SpyInstance;
  let jsonMock: jest.Mock;
  let statusMock: jest.Mock;

  beforeEach(() => {
    filter = new AllExceptionsFilter();
    errorSpy = jest.spyOn(Logger.prototype, 'error').mockImplementation(() => undefined);
    jsonMock = jest.fn();
    statusMock = jest.fn().mockReturnValue({ json: jsonMock });
  });

  afterEach(() => jest.restoreAllMocks());

  function mockHost(method: string, url: string): ArgumentsHost {
    return {
      switchToHttp: () => ({
        getResponse: () => ({ status: statusMock }),
        getRequest: () => ({ method, url }),
      }),
    } as unknown as ArgumentsHost;
  }

  it('includes the ambient correlation id in the error log line', () => {
    requestContext.run({ correlationId: 'corr-err-1' }, () => {
      filter.catch(new BadRequestException('bad input'), mockHost('POST', '/wallets'));
    });

    expect(errorSpy).toHaveBeenCalledWith(
      expect.stringContaining('[corr-err-1] POST /wallets -> 400'),
      expect.any(String),
    );
    expect(statusMock).toHaveBeenCalledWith(400);
  });

  it('logs a placeholder instead of crashing when there is no correlation id', () => {
    filter.catch(new BadRequestException('bad input'), mockHost('GET', '/wallets/1'));

    expect(errorSpy).toHaveBeenCalledWith(
      expect.stringContaining('[-] GET /wallets/1 -> 400'),
      expect.any(String),
    );
  });
});
