const { PrismaClient } = require('@prisma/client')
const { v4: uuidv4 } = require('uuid')

const prisma = new PrismaClient()

// retryTxn wraps SQL operations in an explicit transaction.
// If the transaction fails, the function retries the operation, with exponential backoff.
async function retryTxn (n, max, operation) {
  while (true) {
    n++
    if (n === max) {
      throw new Error('Max retry count reached.')
    }
    try {
      await operation()
      await prisma.$queryRaw`COMMIT;`
      return
    } catch (err) {
      if (err.code !== '40001') { // Transaction retry errors use the SQLSTATE error code 40001 (serialization error)
        return err
      } else {
        console.log('Transaction failed. Retrying transaction.')
        console.log(err.message)
        console.log('Rolling back transaction.')
        await prisma.$queryRaw`ROLLBACK;`
        await new Promise((resolve) => setTimeout(resolve, 2 ** n * 1000))
      }
    }
  }
}

// initTable inserts the input row values into the account table.
async function initTable (rowVals) {
  await prisma.account.createMany({
    data: rowVals,
    skipDuplicates: true
  })
  console.log('Account rows added.')
}

// updateTable updates existing row values in the account table.
async function updateTable () {
  await prisma.account.updateMany({
    where: {
      balance: {
        gt: 100
      }
    },
    data: {
      balance: {
        decrement: 5
      }
    }
  })
  console.log('Account rows updated.')
}

// deleteRows deletes all rows in the account table.
async function deleteRows () {
  const deleteAllRows = await prisma.account.deleteMany()
  return console.log('Account rows deleted.', deleteAllRows)
}

// genVals generates random UUID values and integer values to be inserted into the account table.
async function genVals (n) {
  const accountValues = Array(n)
  let i = 0
  while (i < accountValues.length) {
    const idVal = await uuidv4()
    const balVal = await Math.floor(Math.random() * 1000)
    accountValues[i] = { id: idVal, balance: balVal }
    i++
  }
  return accountValues
}

async function main () {
  const firstAccount = await genVals(10)

  await retryTxn(0, 15, initTable(firstAccount))

  await new Promise(resolve => setTimeout(resolve, 2000)) // wait 2 seconds before selecting and printing rows
  const firstRows = await prisma.account.findMany()
  console.log('Initial row values:\n', firstRows)

  await retryTxn(0, 15, updateTable)

  await new Promise(resolve => setTimeout(resolve, 2000)) // wait 2 seconds before selecting and printing rows
  const updatedRows = await prisma.account.findMany()
  console.log('Updated row values:\n', updatedRows)

  await retryTxn(0, 15, deleteRows)
}

main()
  .catch((e) => {
    throw e
  })
  .finally(async () => {
    await prisma.$disconnect()
  })
