/*

DEBUG=nc* yarn tsn generateTestTable

Creates ./tmp/test.sqlite
Fills it with 1M rows of TestItem
Not gzipped (to better test streaming)

 */

import { Readable } from 'stream'
import { TEST_TABLE } from '@naturalcycles/db-lib/dist/testing'
import { _range } from '@naturalcycles/js-lib'
import { transformLogProgress, writableForEach, _pipeline } from '@naturalcycles/nodejs-lib'
import { runScript } from '@naturalcycles/nodejs-lib/dist/script'
import { SqliteKeyValueDB } from '../src'
import { tmpDir } from '../src/test/paths.cnst'
import { TestItem } from '../src/test/test.model'

runScript(async () => {
  const filename = `${tmpDir}/test.sqlite`
  const db = new SqliteKeyValueDB({ filename })
  // "Better" is 6 seconds vs 14 seconds before
  // const db = new BetterSqliteKeyValueDB({ filename })

  await db.ping()
  await db.createTable(TEST_TABLE, { dropIfExists: true })
  await db.beginTransaction()

  // const items = _range(1, 1_000_001).map(n => ({
  //   id: `id_${n}`,
  //   n,
  //   even: n % 2 === 0,
  // })).map(item => [item.id, Buffer.from(JSON.stringify(item))] as [k: string, v: Buffer])

  // await db.saveBatch(TEST_TABLE, items)

  await _pipeline([
    Readable.from(_range(1, 1_000_001)),
    transformLogProgress({ logEvery: 10_000 }),
    writableForEach(
      async n => {
        const item: TestItem = {
          id: `id_${n}`,
          n,
          even: n % 2 === 0,
        }
        const buf = Buffer.from(JSON.stringify(item))
        // const buf = await gzipString(JSON.stringify(item))

        await db.saveBatch(TEST_TABLE, [[item.id, buf]])
      },
      { concurrency: 16 },
    ),
  ])

  await db.endTransaction()
  await db.close()
})
