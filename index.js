const { PrismaClient } = require('@prisma/client')
const { v4: uuidv4 } = require("uuid")

const prisma = new PrismaClient()

async function retryTxn(n, max, operation) {
    while (true) {
        n++
        if (n === max) {
            throw new Error("Max retry count reached.")
        }
        try {
            await operation()
            await prisma.$queryRaw`COMMIT;`
            return;
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

async function initTable(rowVals) {
    await prisma.accounts.createMany({
        data: rowVals,
        skipDuplicates: true,
    })
    console.log("Initial account values:\n", await prisma.accounts.findMany())
}

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
    console.log("Updated account values:\n", await prisma.accounts.findMany())
}

async function deleteRows() {
    console.log("Account rows deleted.", await prisma.accounts.deleteMany())

}

async function main() {

    var accountValues = Array(5);
    let i = 0;
    while (i < accountValues.length) {
        idVal = await uuidv4()
        balVal = await Math.floor(Math.random() * 1000)
        accountValues[i] = { id: idVal, balance: balVal }
        i++
    }

    await retryTxn(0, 15, initTable(accountValues))

    await retryTxn(0, 15, updateTable)

    await retryTxn(0, 15, deleteRows)

}

main()
    .catch((e) => {
        throw e
    })
    .finally(async () => {
        await prisma.$disconnect()
    })