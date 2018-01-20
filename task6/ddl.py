from clickhouse_driver import Client

client = Client('127.0.0.1')

client.execute('DROP TABLE IF EXISTS test;')

client.execute(
    '''CREATE TABLE IF NOT EXISTS test (
        step UInt64,
        type Enum8('CASH_IN' = 1, 'CASH_OUT' = 2, 'DEBIT' = 3, 'PAYMENT' = 4, 'TRANSFER' = 5),
        amount Float64,
        nameOrig String,
        oldBalanceOrg Float64,
        newBalanceOrg Float64,
        nameDest String,
        oldBalanceDest Float64,
        newBalanceDest Float64,
        isFraud UInt8,
        isFlaggedFraud UInt8
        ) ENGINE = Memory;'''
)
