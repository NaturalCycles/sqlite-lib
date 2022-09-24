import { runCommonKeyValueDBTest, TEST_TABLE } from '@naturalcycles/db-lib/dist/testing'
import { BetterSqliteKeyValueDB } from './betterSqliteKeyValueDB'

const db = new BetterSqliteKeyValueDB({
  filename: ':memory:',
})

beforeAll(async () => {
  // await db.ping()

  // await db.getTables()
  await db.createTable(TEST_TABLE, { dropIfExists: true })
})

afterAll(() => db.close())

describe('runCommonKeyValueDBTest', () => runCommonKeyValueDBTest(db))

test('count', async () => {
  const count = await db.count(TEST_TABLE)
  expect(count).toBe(0)
})
