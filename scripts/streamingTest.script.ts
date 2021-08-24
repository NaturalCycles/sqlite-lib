/*

DEBUG=nc* yarn tsn streamingTest

 */

import { TEST_TABLE } from '@naturalcycles/db-lib/dist/testing'
import { transformLogProgress, writableForEach, _pipeline } from '@naturalcycles/nodejs-lib'
import { runScript } from '@naturalcycles/nodejs-lib/dist/script'
import { SqliteKeyValueDB } from '../src'
import { tmpDir } from '../src/test/paths.cnst'

runScript(async () => {
  const db = new SqliteKeyValueDB({
    filename: `${tmpDir}/test.sqlite`,
  })
  await db.open()

  await _pipeline([
    db.streamIds(TEST_TABLE, 50_000),
    // db.streamValues(TEST_TABLE, 50_000),
    // db.streamEntries(TEST_TABLE, 50_000),
    transformLogProgress({ logEvery: 10_000 }),
    // writableForEach<KeyValueTuple>(async ([id, v]) => {
    //   // console.log(id, JSON.parse(v.toString()))
    // }),
    // writableForEach<Buffer>(async v => {
    //   // console.log(JSON.parse(v.toString()))
    // }),
    writableForEach<string>(async _id => {
      //
    }),
  ])

  await db.close()
})
