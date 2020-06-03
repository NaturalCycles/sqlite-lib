import { runCommonKVDBTest, TEST_TABLE } from '@naturalcycles/db-lib/dist/testing'
import { SQLiteKVDB } from './sqlite.kv.db'

const db = new SQLiteKVDB({
  filename: ':memory:',
  // filename: `${tmpDir}/testdb.sqlite`,
})

beforeAll(async () => {
  await db.ping()

  // await db.getTables()
  await db.createTable(TEST_TABLE, { dropIfExists: true })
})

afterAll(async () => await db.close())

describe('runCommonKVDBTest', () => runCommonKVDBTest(db))

// test('test1', async () => {
//   await db.deleteByIds(TEST_TABLE, ['id1', 'id2'])
//   await db.saveBatch(TEST_TABLE, {
//     k1: Buffer.from('hello1'),
//     k2: Buffer.from('hello2'),
//   })
// })
