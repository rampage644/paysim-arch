import psycopg2

# Connect to an existing database
conn = psycopg2.connect("dbname=test user=dbuser host=127.0.0.1 password=secret")

# Open a cursor to perform database operations
cur = conn.cursor()

# Execute a command: this creates a new table
ddls = [
    'DROP TABLE IF EXISTS account;',
    'DROP TABLE IF EXISTS fraud_type;',
    'DROP TABLE IF EXISTS transaction_type;',
    'DROP TABLE IF EXISTS transaction;',
    '''CREATE TABLE IF NOT EXISTS account (id serial PRIMARY KEY, name varchar);''',
    '''CREATE TABLE IF NOT EXISTS fraud_type (id serial PRIMARY KEY, ftype integer, description varchar);''',
    '''CREATE TABLE IF NOT EXISTS transaction_type (id serial PRIMARY KEY, name varchar);''',
    '''CREATE TABLE IF NOT EXISTS transaction (
        id serial PRIMARY KEY,
        origAccountKey int,
        destAccountKey int,
        typeKey int,
        fraudKey int,
        amount double precision,
        timestamp bigserial,
        origBalanceOld double precision,
        origBalanceNew double precision,
        destBalanceOld double precision,
        destBalanceNew double precision
);''',
]

for ddl in ddls:
    cur.execute(ddl)

# Make the changes to the database persistent
conn.commit()

# Close communication with the database
cur.close()
conn.close()
