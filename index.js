const { PrismaClient } = require('@prisma/client')
const { v4: uuidv4 } = require("uuid")

const prisma = new PrismaClient()

// retryTxn wraps SQL operations in an explicit transaction. 
// If the transaction fails, the function retries the operation, with exponential backoff.
async function retryTxn(n, max, operation) {
    while (true) {
        n++
        if (n === max) {
            throw new Error("Max retry count reached.")
        }
        try {
            await operation()
            await prisma.$queryRaw`COMMIT;`
            return
        } catch (err) {
            if (err.code !== "40001") {
                return err
            } else {
                console.log("Transaction failed. Retrying transaction.")
                console.log(err.message)
                console.log("Rolling back transaction.")
                await prisma.$queryRaw`ROLLBACK;`
                await new Promise((r) => setTimeout(r, 2 ** n * 1000))
            }
        }
    }
}

// initTable inserts the input row values into the accounts table.
async function initTable(rowVals) {
    await prisma.accounts.createMany({
        data: rowVals,
        skipDuplicates: true,
    })
    console.log("Account rows added.")
    return
}

// updateTable updates existing row values in the accounts table.
async function updateTable() {
    await prisma.accounts.updateMany({
        where: {
            balance: {
                gt: 100,
            }
        },
        data: {
            balance: {
                decrement: 5
            }
        },
    })
    console.log("Account rows updated.")
    return
}

// deleteRows deletes all rows in the accounts table.
async function deleteRows() {
    const deleteAllRows = await prisma.accounts.deleteMany()
    return console.log("Account rows deleted.", deleteAllRows)
}

// genVals generates random UUID values and integer values to be inserted into the accounts table.
async function genVals(n) {
    var accountValues = Array(n);
    let i = 0;
    while (i < accountValues.length) {
        idVal = await uuidv4()
        balVal = await Math.floor(Math.random() * 1000)
        accountValues[i] = { id: idVal, balance: balVal }
        i++
    }
    return accountValues
}

async function main() {

    const firstAccounts = await genVals(10)

    await retryTxn(0, 15, initTable(firstAccounts))

    await new Promise(r => setTimeout(r, 2000)); // wait 2 seconds before selecting and printing rows
    const firstRows = await prisma.accounts.findMany()
    console.log("Initial row values:\n", firstRows)

    await retryTxn(0, 15, updateTable)

    await new Promise(r => setTimeout(r, 2000)); // wait 2 seconds before selecting and printing rows
    const updatedRows = await prisma.accounts.findMany()
    console.log("Updated row values:\n", updatedRows)

    await retryTxn(0, 15, deleteRows)

}

main()
    .catch((e) => {
        throw e
    })
    .finally(async () => {
        await prisma.$disconnect()
    })