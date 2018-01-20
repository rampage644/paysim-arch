from clickhouse_driver import Client

client = Client('172.17.0.1')

# step,type,amount,nameOrig,oldbalanceOrg,newbalanceOrig,nameDest,oldbalanceDest,newbalanceDest,isFraud,isFlaggedFraud
# 1,PAYMENT,9839.64,C1231006815,170136.0,160296.36,M1979787155,0.0,0.0,0,0

res = client.execute(
    '''CREATE TABLE IF NOT EXISTS test (
        step UInt64,
        type Enum8('CASH-IN' = 1, 'CASH-OUT' = 2, 'DEBIT' = 3, 'PAYMENT' = 4, 'TRANSFER' = 5),
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

print(res)

# client.execute(
#     'INSERT INTO test (x) VALUES',
#     [{'x': 1}, {'x': 2}, {'x': 3}, {'x': 100}]
# )
# client.execute('INSERT INTO test (x) VALUES', [[200]])

# print(client.execute('SELECT sum(x) FROM test'))
