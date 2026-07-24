import { Injectable, NestMiddleware } from '@nestjs/common';
import { NextFunction, Request, Response } from 'express';
import { v4 as uuidv4 } from 'uuid';
import { requestContext } from '../context/request-context';

export const CORRELATION_ID_HEADER = 'x-correlation-id';

@Injectable()
export class CorrelationIdMiddleware implements NestMiddleware {
  use(req: Request, res: Response, next: NextFunction) {
    const correlationId = (req.headers[CORRELATION_ID_HEADER] as string) || uuidv4();
    res.setHeader(CORRELATION_ID_HEADER, correlationId);
    // Everything downstream for this request - guards, pipes, the controller,
    // services, the logging interceptor, the exception filter - runs inside
    // this context, so getCorrelationId() resolves anywhere in that chain
    // without threading the id through method signatures.
    requestContext.run({ correlationId }, next);
  }
}
