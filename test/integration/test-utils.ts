import { INestApplication, ValidationPipe } from '@nestjs/common';
import { getConnectionToken, getModelToken } from '@nestjs/mongoose';
import { Test } from '@nestjs/testing';
import * as bcrypt from 'bcrypt';
import { Connection } from 'mongoose';
import request from 'supertest';
import { AppModule } from '../../src/app.module';
import { RedisService } from '../../src/redis/redis.service';

// These integration tests exercise the real Mongo/Redis/RabbitMQ stack started by
// `docker-compose up -d mongo redis rabbitmq`. They are skipped implicitly if those
// services are unreachable (the relevant `beforeAll` will throw and fail loudly).
process.env.MONGODB_URI =
  process.env.MONGODB_URI ||
  'mongodb://localhost:27017/wallet-platform-integration?replicaSet=rs0&directConnection=true';
process.env.REDIS_HOST = process.env.REDIS_HOST || 'localhost';
process.env.REDIS_PORT = process.env.REDIS_PORT || '6379';
process.env.RABBITMQ_URI = process.env.RABBITMQ_URI || 'amqp://guest:guest@localhost:5672';
process.env.JWT_SECRET = process.env.JWT_SECRET || 'integration-test-secret';

export async function createTestApp() {
  const moduleRef = await Test.createTestingModule({ imports: [AppModule] }).compile();
  const app = moduleRef.createNestApplication();
  app.useGlobalPipes(new ValidationPipe({ transform: true, whitelist: true }));
  await app.init();

  const connection = app.get<Connection>(getConnectionToken());
  return { app, connection };
}

export async function resetDatabase(connection: Connection) {
  const collections = await connection.db!.collections();
  await Promise.all(collections.map((collection) => collection.deleteMany({})));
}

// Rate-limit counters live in the same real, shared Redis instance across
// every integration spec file (unlike Mongo, which resetDatabase wipes per
// file) - a route hit from more than one file (e.g. POST /wallets/transfer,
// used by both transfer-flow and rate-limiting specs) would otherwise
// accumulate against the same throttle key across files/runs.
export async function flushThrottleState(app: INestApplication) {
  await app.get(RedisService).getClient().flushdb();
}

// Fetches the real Mongoose model bound to the running app, so lookups get
// schema-aware casting (e.g. string id -> ObjectId) instead of the raw driver's
// literal string match against `connection.collection(...)`.
export function getModel(app: INestApplication, modelName: string) {
  return app.get(getModelToken(modelName));
}

export async function createAuthenticatedRequest(app: any, connection: Connection) {
  const email = `integration-${Date.now()}@wallet-platform.test`;
  const password = 'Password123!';

  await connection.collection('users').insertOne({
    email,
    passwordHash: await bcrypt.hash(password, 10),
    fullName: 'Integration Test User',
    createdAt: new Date(),
    updatedAt: new Date(),
  });

  const loginResponse = await request(app.getHttpServer())
    .post('/auth/login')
    .send({ email, password })
    .expect(200);

  const token: string = loginResponse.body.accessToken;

  return {
    get: (url: string) =>
      request(app.getHttpServer()).get(url).set('Authorization', `Bearer ${token}`),
    post: (url: string) =>
      request(app.getHttpServer()).post(url).set('Authorization', `Bearer ${token}`),
  };
}
