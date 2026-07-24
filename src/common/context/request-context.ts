import { AsyncLocalStorage } from 'async_hooks';

export interface RequestContextStore {
  correlationId?: string;
}

// The one primitive everything else reads from - no threading a correlation
// id through method signatures anywhere else in the codebase. Populated by
// CorrelationIdMiddleware for HTTP requests, and re-established by
// TransferEventsConsumer when processing a message that carries one.
export const requestContext = new AsyncLocalStorage<RequestContextStore>();

export function getCorrelationId(): string | undefined {
  return requestContext.getStore()?.correlationId;
}
