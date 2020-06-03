import { createTestItemsDBM, TEST_TABLE } from '@naturalcycles/db-lib/dist/testing'
import { insertSQL } from './query.util'

test('insertSQL', () => {
  const items = createTestItemsDBM(3)
  const sqls = insertSQL(TEST_TABLE, items)
  console.log(sqls)
  // sqls.forEach(s => console.log(s.values))
})
