import { runCommonKeyValueDBTest, TEST_TABLE } from '@naturalcycles/db-lib/dist/testing'
import { SqliteKeyValueDB } from './sqliteKeyValueDB'

const db = new SqliteKeyValueDB({
  filename: ':memory:',
  // filename: `${tmpDir}/testdb.sqlite`,
})

beforeAll(async () => {
  await db.ping()

  // await db.getTables()
  await db.createTable(TEST_TABLE, { dropIfExists: true })
})

afterAll(async () => await db.close())

describe('runCommonKeyValueDBTest', () => runCommonKeyValueDBTest(db))

test('count', async () => {
  const count = await db.count(TEST_TABLE)
  expect(count).toBe(0)
})

// test('test1', async () => {
//   await db.deleteByIds(TEST_TABLE, ['id1', 'id2'])
//   await db.saveBatch(TEST_TABLE, {
//     k1: Buffer.from('hello1'),
//     k2: Buffer.from('hello2'),
//   })
// })
