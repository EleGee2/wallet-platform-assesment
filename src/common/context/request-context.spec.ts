import { getCorrelationId, requestContext } from './request-context';

describe('request-context', () => {
  it('returns undefined outside any context', () => {
    expect(getCorrelationId()).toBeUndefined();
  });

  it('returns the correlation id set for the current context', async () => {
    await requestContext.run({ correlationId: 'corr-123' }, async () => {
      expect(getCorrelationId()).toBe('corr-123');
    });
  });

  it('is available across an async gap within the same context', async () => {
    await requestContext.run({ correlationId: 'corr-456' }, async () => {
      await new Promise((resolve) => setTimeout(resolve, 1));
      expect(getCorrelationId()).toBe('corr-456');
    });
  });

  it('does not leak between sibling contexts', async () => {
    await Promise.all([
      requestContext.run({ correlationId: 'a' }, async () => {
        await new Promise((resolve) => setTimeout(resolve, 5));
        expect(getCorrelationId()).toBe('a');
      }),
      requestContext.run({ correlationId: 'b' }, async () => {
        expect(getCorrelationId()).toBe('b');
      }),
    ]);
  });

  it('returns undefined again once the context ends', async () => {
    await requestContext.run({ correlationId: 'corr-789' }, async () => undefined);
    expect(getCorrelationId()).toBeUndefined();
  });
});
