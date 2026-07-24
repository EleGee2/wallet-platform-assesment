import { Request, Response } from 'express';
import { getCorrelationId } from '../context/request-context';
import { CORRELATION_ID_HEADER, CorrelationIdMiddleware } from './correlation-id.middleware';

describe('CorrelationIdMiddleware', () => {
  let middleware: CorrelationIdMiddleware;
  let setHeaderMock: jest.Mock;

  beforeEach(() => {
    middleware = new CorrelationIdMiddleware();
    setHeaderMock = jest.fn();
  });

  function mockRequest(headers: Record<string, string> = {}): Request {
    return { headers } as unknown as Request;
  }

  function mockResponse(): Response {
    return { setHeader: setHeaderMock } as unknown as Response;
  }

  it('generates a new correlation id when no header is present, and sets it on the response', () => {
    const next = jest.fn();

    middleware.use(mockRequest(), mockResponse(), next);

    expect(setHeaderMock).toHaveBeenCalledWith(
      CORRELATION_ID_HEADER,
      expect.stringMatching(/^[0-9a-f-]{36}$/),
    );
    expect(next).toHaveBeenCalled();
  });

  it('reuses an incoming correlation id header instead of generating a new one', () => {
    const next = jest.fn();

    middleware.use(mockRequest({ [CORRELATION_ID_HEADER]: 'incoming-id' }), mockResponse(), next);

    expect(setHeaderMock).toHaveBeenCalledWith(CORRELATION_ID_HEADER, 'incoming-id');
  });

  it('calls next from inside a context where getCorrelationId() resolves correctly', () => {
    let observedDuringNext: string | undefined;
    const next = jest.fn(() => {
      observedDuringNext = getCorrelationId();
    });

    middleware.use(mockRequest({ [CORRELATION_ID_HEADER]: 'ctx-id' }), mockResponse(), next);

    expect(observedDuringNext).toBe('ctx-id');
    // The context shouldn't leak past the middleware's own call.
    expect(getCorrelationId()).toBeUndefined();
  });
});
