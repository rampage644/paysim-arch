#%%
import os
import csv
import psycopg2
import pandas
from sqlalchemy import create_engine

source = os.path.join(os.path.dirname(__file__), '..', 'PS_20174392719_1491204439457_log_10k.csv')


conn = psycopg2.connect("dbname=test user=dbuser host=127.0.0.1 password=secret")
cur = conn.cursor()

# preparation steps: populate transaction transaction_type, fraud_type and account tables
# imitate we already have some customers in account table
df = pandas.read_csv(source, usecols=['nameOrig', 'nameDest'])
customers = pandas.concat([df.nameDest, df.nameOrig], axis=0).sample(frac=0.7)

cur.execute('DELETE FROM account;')
conn.commit()
for customer in customers:
    cur.execute('INSERT INTO account (name) VALUES (%s)', (customer,))
conn.commit()

cur.execute('SELECT id, name from account;')
# cache/dict for customer quick search
customer_cache = {record[1]: record[0] for record in cur}


ftype_cache = {
    0: 1, # isFraud = 0 isFlaggedFraud = 0
    1: 2, # isFraud = 1 isFlaggedFraud = 0
    2: 3, # isFraud = 0 isFlaggedFraud = 1
    3: 4  # isFraud = 1 isFlaggedFraud = 1
}

try:
    for k, v in ftype_cache.items():
        cur.execute('INSERT INTO fraud_type (id, ftype) VALUES(%s, %s)', (v, k))
    conn.commit()
except psycopg2.IntegrityError as e:
    print(e)
finally:
    conn.rollback()


ttype_cache = {
    'PAYMENT':  1,
    'TRANSFER': 2,
    'CASH_OUT': 3,
    'DEBIT':    4,
    'CASH_IN':  5
}

try:
    for k, v in ttype_cache.items():
        cur.execute('INSERT INTO transaction_type (id, name) VALUES(%s, %s)', (v, k))
    conn.commit()
except psycopg2.IntegrityError as e:
    print(e)
finally:
    conn.rollback()


def get_customer_id(customer):
    if customer_cache.get(customer, None) is not None:
        return customer_cache[customer]
    cur.execute('INSERT INTO account (name) VALUES (%s) RETURNING id', (customer,))
    cust_id = cur.fetchone()[0]
    customer_cache[customer] = cust_id
    return cust_id

# main function
with open(source, newline='') as sfile:
    rdr = csv.DictReader(sfile)


    for idx, entry in enumerate(rdr):
        orig_acc_id = get_customer_id(entry['nameOrig'])
        des_acc_id = get_customer_id(entry['nameDest'])
        fraud_type = ftype_cache.get(entry['isFraud'] + 2 * entry['isFlaggedFraud'], 0)
        transaction_type = ttype_cache.get(entry['type'])
        cur.execute('''INSERT INTO transaction (
            origAccountKey, destAccountKey, typeKey, fraudKey, amount, timestamp,
            origBalanceOld, origBalanceNew, destBalanceOld, destBalanceNew) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )''', (orig_acc_id, des_acc_id, transaction_type, fraud_type,
                entry['amount'], entry['step'], entry['oldbalanceOrg'],
                    entry['newbalanceOrig'], entry['oldbalanceDest'], entry['newbalanceDest']))
        if idx % 1000:
            conn.commit()

    conn.commit()

cur.close()
conn.close()
