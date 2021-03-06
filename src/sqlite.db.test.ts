import {
  createTestItemsDBM,
  TEST_TABLE,
  testItemDBMJsonSchema,
} from '@naturalcycles/db-lib/dist/testing'
import { SQLiteDB } from './sqlite.db'

test('test1', async () => {
  const db = new SQLiteDB({
    filename: ':memory:',
  })

  await db.ping()

  // await db.getTables()
  await db.createTable(TEST_TABLE, testItemDBMJsonSchema.build(), { dropIfExists: true })

  const items = createTestItemsDBM(3)
  await db.saveBatch(TEST_TABLE, items)

  await db.close()
})
