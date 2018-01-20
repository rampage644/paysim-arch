# Paysim

Paysim dataset is available at <https://www.kaggle.com/ntnu-testimon/paysim1/data>. Below is the data platform architecture design considerations for it.

## High level design

As the data is financial transactions is a must here (there is even such term in financial world). I have two possible options of doing it:

 1. First one is have OLTP database as a gatekeeper for whole system -- it will ensure no declined transactions are allowed in. Here by 'declined' I mean pure technical reason, such as competing transactions for, say, one account. It has nothing to do to with transaction being declined by, for example, financial institution because of fraud. After transaction is accepted it's fed into message queue for other consumers.
 1. Second one is directly put transaction (which could consist of multiple rows/entries) into message queue and have one high priority consumer listening for them and working with the OLTP database. In case transaction is declined, another message is put into the queue reverting previous one. This is the fallback option when first suggestion is not the option.

Message queue allows us to separate concerns and postpone data-processing tasks. For analytical datawarehouse task we will have one consumer that is triggered with some period, reads the data for the previous period (say, 1 hour) and processes/aggregates the data and puts it into OLAP database. For reports generation purposes, where one needs quickly locate particular data records another consumer watches for new data arrival and reroutes the data to another store.

This solution allows one to quickly add more consumers/sinks for the data. One example is near real-time fraud detection algorithm. Another advantage is to share the workload and allow fine tuning different parts for specific tasks and performance requirements.

![Architecture](task1/arch1.png).

Now, let's step to concrete tools and solutions (marked on the diagram as red numbers with asterisks).

I'd like to note that utilizing message queue as a mediator, cloud solutions could be employed as well.

 1. Application API calls to database (language of choice)
 1. OLTP database: Microsoft SQL if allowed, PostgreSQL/MySQL if open-source is a requirement, NewSQL guys (CockroachDB/TiDB) if brave, VoltDB.
 1. The way data flows from operational database to message queue. Two possible options I'd like to suggest. First one lets decouple from application API - operate on transaction log of the database. That is, once transaction is commited get notified and extract last information to write to the message queue (mq producer). Another option (if first one is technically not feasible) to maintain log table in the database and use stored procedures to simultaneously append transactions with operation table change.
 1. **Kafka**. Other possible options is to have some ETL framework/solution to deliver data from operational database to other sources. Examples could be batched ones like Informatica ETL and Microsoft SSIS, stream-oriented like Apache Flume, Apache Storm (both with the help of Apache Airflow or other orchestrating tool) and probably dozens of others I'm not aware of.
 1. Distributed filesystem - **HDFS**, Ceph, S3/Glacier, GFS. Stored as compressed files: gzipped csv or in ORC/Avro/Parquet format.
 1. Yandex **ClickHouse**, MailRU Tarantool, HyPer, VoltDB, apache Ignite, MemSQL. It would effectively by the copy of operational DB log table. Those next generation in-memory databases which claim to be Hybrid DBMS.
 1. OLAP database could be read as database with columnar store engine. HBase as open source version of Google BigTable. Actually, **ClickHouse** is also a good fit here. But Microsoft SSAS is still strong candidate here. Another good open source option is Postgres tuned for OLAP (well, depending on scale). Apache HAWQ (easily integrate with hdfs and Avro/Parquet files), Greenplum.

## Streaming data processing

Here we assume the data is somehow delivered to us from operational database transaction log. So the task is to put the data into our system. According to previous choice (3rd and 4th points on the diagram) it's translated to write a Kafka producer code. However, Kafka has Connenctor API that could be used for this task as well.

I've chosen Kafka as I believe there are no tool like that exists: distributed, robust, message queue. I didn't like other ETL tools because of concern separation and ease of changes for data platform. For more full list, see previous section.

```
# download data
unzip PS_20174392719_1491204439457_log.csv.zip
head -n10000 PS_20174392719_1491204439457_log.csv > PS_20174392719_1491204439457_log_10k.csv

# run kafka
cd task2/
git clone git@github.com:wurstmeister/kafka-docker.git
cd kafka-docker/
# first, change IP in docker-compose-single-broker for docker host address (ex, 172.17.0.1)
docker-compose -f docker-compose-single-broker.yml up -d
cd ../

# install kafka python client
virtualenv .venv/
source .venv/bin/activate
pip install kafka-python
python3 producer.py

# check output with kafkacat
docker run --rm -it ryane/kafkacat -C -b 172.17.0.1:9092 -t test
```

## Datawarehouse

![Entity Relation Diagram](task3/er.png)

Corresponding DDL statements assume (for simplicity) Postgres is our OLAP database of choice.

```
# run Postgres
docker run --name postgresql -it --restart always \
  --publish 5432:5432 \
  --env 'DB_NAME=test' \
  --env 'DB_USER=dbuser' --env 'DB_PASS=secret' \
  --volume /srv/docker/postgresql:/var/lib/postgresql \
  sameersbn/postgresql:9.6-2

# Perform DDL and ETL
cd task3/
virtualenv .venv/
source .venv/bin/activate
pip install psycopg2 pandas

# Create tables
python ddl.py

# Perform ETL
python etl.py
```


## Hadoop implementation

Hadoop is essentially HDFS (storage) + MapReduce (compute). We will need to store 2.5M transaction per day.

```
$ spark-shell
scala> val df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("../PS_20174392719_1491204439457_log.csv")
scala> df.format("parquet").option("compression", "gzip").save("transactions")
scala> :quit
$ ll transactions
Permissions Size User Date Modified Name
.rw-r--r--     0 ramp 20 Jan 15:08  _SUCCESS
.rw-r--r--   45M ramp 20 Jan 15:08  part-00000-f432dc30-41a8-4530-a377-e356989863d8-c000.gz.parquet
.rw-r--r--   45M ramp 20 Jan 15:08  part-00001-f432dc30-41a8-4530-a377-e356989863d8-c000.gz.parquet
.rw-r--r--   45M ramp 20 Jan 15:08  part-00002-f432dc30-41a8-4530-a377-e356989863d8-c000.gz.parquet
.rw-r--r--   44M ramp 20 Jan 15:08  part-00003-f432dc30-41a8-4530-a377-e356989863d8-c000.gz.parquet
```

So, for given data which is 6,362,621 transactions equals 180M of data. We will assume 2.5M transaction daily require 71M of storage, 1 year of transaction data then require 71M * 365 requires 25G of storage.

Hadoop by default has 3x replication ratio, so, storage capacity triples to 75G. Taking into account that we will need extra space for intermediate MapReduce results (materialization), we will reserve 25% capacity (see Dell-Cloudera reference architecture). Total storage requirements then become 75 / (1 - 0.25) = 100G.

100G per year. 1TB per 10 years.

Let's assume (to make numbers look significant) we have 30% yearly growth, then for 10 years 1.34TB (100G * 1.3 ^ 10) of storage space will be needed. How about 5-6 AWS H1 instances (up to 16TB of storage each)? Should be enough in terms of space. Those also come with 32/64/128/256 RAM in case one wants to share cluster resources with compute-intensive applications, like Spark.

For master nodes, there is no storage requirements so almost any decent configutartion would suffice.

```
cd task4/
sbt package
/opt/apache-spark/bin/spark-submit --class 'Ingest' --master 'local[4]' --executor-memory 4G target/scala-2.11/ingest_2.11-1.0.jar  ../PS_20174392719_1491204439457_log.csv
```



![Hadoop diagram](task4/arch4.png)

## Spark Job

```
cd task5/
sbt package
/opt/apache-spark/bin/spark-submit --class 'TaskN5' --master 'local[4]' target/scala-2.11/task-5_2.11-1.0.jar ../PS_20174392719_1491204439457_log.csv
```

This spark job will:

 1. Calculate each account total payments and save into csv file (not merged, i.e. partitioned)
 1. Show 10 most active accounts

## Fast Searching Engine

Using Yandex Clickhouse. My primary reasoning was it's used by Yandex internally for their metrics transactions and by [Cloudflare](https://www.slideshare.net/Altinity/clickhouse-at-cloudflare-by-marek-vavrusa-79948904) for their DNS requests -- so performance should be OK. Other tools are already described in "High level" section.

```
# run server
cd task6/
# install clickhouse bindings
virtualenv .venv/
source .venv/bin/activate
pip install clickhouse-driver

# run server
# I had problems with docker container connectivity, so instead installed clickhouse locally
# docker run -d --name test --ulimit nofile=262144:262144 yandex/clickhouse-server
systemctl start clickhouse-server.service

# run ingestion
python task6/ddl.py

# load data into clickhouse
tail -n +2 ../PS_20174392719_1491204439457_log.csv | clickhouse-client --query="INSERT INTO test  FORMAT CSV"
```

Here is the result of some query:
```
:) select * from test where nameDest='C1146577120';

SELECT *
FROM test
WHERE nameDest = 'C1146577120'

┌─step─┬────type─┬───amount─┬─nameOrig────┬─oldBalanceOrg─┬──────newBalanceOrg─┬─nameDest────┬────oldBalanceDest─┬─newBalanceDest─┬─isFraud─┬─isFlaggedFraud─┐
│  283 │ CASH_IN │ 61532.07 │ C1252512488 │    2942760.69 │ 3004292.7600000002 │ C1146577120 │ 8562968.729999999 │     8501436.66 │       0 │              0 │
└──────┴─────────┴──────────┴─────────────┴───────────────┴────────────────────┴─────────────┴───────────────────┴────────────────┴─────────┴────────────────┘
┌─step─┬─────type─┬─────────────amount─┬─nameOrig────┬──────oldBalanceOrg─┬──────newBalanceOrg─┬─nameDest────┬─────oldBalanceDest─┬─────newBalanceDest─┬─isFraud─┬─isFlaggedFraud─┐
│    8 │ CASH_OUT │ 234790.47999999998 │ C564847767  │               2333 │                  0 │ C1146577120 │               6487 │          211979.93 │       0 │              0 │
│    8 │  CASH_IN │ 32497.940000000002 │ C493440410  │ 10619846.759999996 │ 10652344.689999998 │ C1146577120 │ 241277.47999999998 │          211979.93 │       0 │              0 │
│    8 │ CASH_OUT │  51144.78999999999 │ C1755884686 │                311 │                  0 │ C1146577120 │          160835.14 │          211979.93 │       0 │              0 │
│    9 │ CASH_OUT │          209245.44 │ C1700382878 │           12293.08 │                  0 │ C1146577120 │          211979.93 │  712955.8300000001 │       0 │              0 │
│    9 │ CASH_OUT │          155269.41 │ C1166115254 │                  0 │                  0 │ C1146577120 │          421225.37 │  712955.8300000001 │       0 │              0 │
│    9 │ CASH_OUT │          249266.12 │ C1876649539 │                  0 │                  0 │ C1146577120 │  576494.7799999999 │  712955.8300000001 │       0 │              0 │
│    9 │  CASH_IN │          112805.06 │ C1766331759 │          7151987.8 │  7264792.859999999 │ C1146577120 │          825760.89 │  712955.8300000001 │       0 │              0 │
│    9 │ CASH_OUT │  581042.1699999999 │ C1856068593 │                  0 │                  0 │ C1146577120 │  712955.8300000001 │ 3074943.7600000002 │       0 │              0 │
│    9 │ TRANSFER │         1205744.44 │ C1162490331 │              15703 │                  0 │ C1146577120 │            1293998 │ 3074943.7600000002 │       0 │              0 │
│    9 │ CASH_OUT │  575201.3200000001 │ C35439193   │         2905036.77 │ 2329835.4499999997 │ C1146577120 │         2499742.44 │ 3074943.7600000002 │       0 │              0 │
│   10 │  CASH_IN │          261734.64 │ C687058609  │              25333 │ 287067.63999999996 │ C1146577120 │ 3074943.7600000002 │         2468477.07 │       0 │              0 │
│   11 │  CASH_IN │          344732.05 │ C908470304  │             100713 │          445445.05 │ C1146577120 │         2813209.12 │         2468477.07 │       0 │              0 │
│   12 │ CASH_OUT │          231989.64 │ C302026242  │                  0 │                  0 │ C1146577120 │         2468477.07 │            2361079 │       0 │              0 │
│   13 │  CASH_IN │          339387.71 │ C641964371  │         5219003.56 │ 5558391.2700000005 │ C1146577120 │         2700466.71 │            2361079 │       0 │              0 │
│   13 │ TRANSFER │          1126252.2 │ C556598116  │                  0 │                  0 │ C1146577120 │            2361079 │          3487331.2 │       0 │              0 │
│   14 │ CASH_OUT │          136976.37 │ C512630681  │             159568 │ 22591.629999999997 │ C1146577120 │          3487331.2 │          3559645.5 │       0 │              0 │
│   15 │  CASH_IN │           64662.07 │ C316722829  │ 15387572.769999998 │ 15452234.839999996 │ C1146577120 │         3624307.57 │          3559645.5 │       0 │              0 │
│   15 │  CASH_IN │           20781.47 │ C9083572    │  9164758.229999999 │          9185539.7 │ C1146577120 │          3559645.5 │         3538864.03 │       0 │              0 │
│   18 │ TRANSFER │ 50175.159999999996 │ C78741416   │          271439.85 │          221264.69 │ C1146577120 │         3538864.03 │         3825621.56 │       0 │              0 │
│   18 │ CASH_OUT │          236582.36 │ C1814267614 │              52033 │                  0 │ C1146577120 │          3589039.2 │         3825621.56 │       0 │              0 │
│   19 │ CASH_OUT │          334938.17 │ C1082358645 │                  0 │                  0 │ C1146577120 │         3825621.56 │         4160559.73 │       0 │              0 │
│   22 │ CASH_OUT │  43158.78999999999 │ C446303289  │                  0 │                  0 │ C1146577120 │         4160559.73 │         4203718.52 │       0 │              0 │
│   23 │ CASH_OUT │          226861.91 │ C962724862  │                  0 │                  0 │ C1146577120 │         4203718.52 │  4430580.430000001 │       0 │              0 │
│   34 │ CASH_OUT │           57218.82 │ C1693643803 │                  0 │                  0 │ C1146577120 │  4430580.430000001 │         4519809.84 │       0 │              0 │
│   35 │ CASH_OUT │           32010.59 │ C869991914  │                  0 │                  0 │ C1146577120 │         4487799.25 │         4519809.84 │       0 │              0 │
│   38 │ CASH_OUT │          208972.78 │ C1857319446 │           64752.87 │                  0 │ C1146577120 │         4519809.84 │         4728782.63 │       0 │              0 │
│   41 │ CASH_OUT │          230165.91 │ C1297721681 │                  0 │                  0 │ C1146577120 │         4728782.63 │         4958948.53 │       0 │              0 │
│   42 │ CASH_OUT │          264210.21 │ C1076867001 │             194700 │                  0 │ C1146577120 │         4958948.53 │         5223158.75 │       0 │              0 │
└──────┴──────────┴────────────────────┴─────────────┴────────────────────┴────────────────────┴─────────────┴────────────────────┴────────────────────┴─────────┴────────────────┘
┌─step─┬─────type─┬─────────────amount─┬─nameOrig────┬──────oldBalanceOrg─┬──────newBalanceOrg─┬─nameDest────┬─────oldBalanceDest─┬─────newBalanceDest─┬─isFraud─┬─isFlaggedFraud─┐
│   96 │ CASH_OUT │          186456.54 │ C260310735  │                  0 │                  0 │ C1146577120 │         5223158.75 │         5409615.29 │       0 │              0 │
│  130 │ CASH_OUT │          238326.93 │ C1476108694 │                  0 │                  0 │ C1146577120 │         5409615.29 │         5647942.22 │       0 │              0 │
│  133 │ CASH_OUT │          103058.23 │ C1619885373 │              22253 │                  0 │ C1146577120 │         5647942.22 │         5751000.45 │       0 │              0 │
│  134 │ TRANSFER │          453784.95 │ C218992633  │                  0 │                  0 │ C1146577120 │         5751000.45 │          6204785.4 │       0 │              0 │
│  136 │ CASH_OUT │          792344.98 │ C672302897  │                  0 │                  0 │ C1146577120 │          6204785.4 │         6997130.38 │       0 │              0 │
│  155 │ CASH_OUT │  9965.359999999999 │ C834237233  │                  0 │                  0 │ C1146577120 │         6997130.38 │ 6730788.1899999995 │       0 │              0 │
│  156 │  CASH_IN │          276307.55 │ C1011485176 │              85267 │          361574.55 │ C1146577120 │         7007095.74 │ 6730788.1899999995 │       0 │              0 │
│  160 │  CASH_IN │           421971.7 │ C1402054316 │         2675273.34 │         3097245.04 │ C1146577120 │ 6730788.1899999995 │         6308816.49 │       0 │              0 │
│  164 │ TRANSFER │  872939.3500000001 │ C593049833  │                  0 │                  0 │ C1146577120 │         6308816.49 │         7181755.84 │       0 │              0 │
│  166 │ CASH_OUT │           245041.5 │ C936901479  │               9005 │                  0 │ C1146577120 │         7181755.84 │         7426797.33 │       0 │              0 │
│  177 │  CASH_IN │ 103124.87000000001 │ C1055034831 │ 10064928.989999998 │ 10168053.859999998 │ C1146577120 │         7426797.33 │  7323672.470000001 │       0 │              0 │
│  179 │  CASH_IN │           80072.82 │ C1692898919 │                251 │           80323.82 │ C1146577120 │  7323672.470000001 │  7243599.649999999 │       0 │              0 │
└──────┴──────────┴────────────────────┴─────────────┴────────────────────┴────────────────────┴─────────────┴────────────────────┴────────────────────┴─────────┴────────────────┘
┌─step─┬─────type─┬────amount─┬─nameOrig───┬─oldBalanceOrg─┬─newBalanceOrg─┬─nameDest────┬─oldBalanceDest─┬─newBalanceDest─┬─isFraud─┬─isFlaggedFraud─┐
│  466 │ CASH_OUT │ 181591.66 │ C318512050 │        105991 │             0 │ C1146577120 │     8501436.66 │     8683028.32 │       0 │              0 │
└──────┴──────────┴───────────┴────────────┴───────────────┴───────────────┴─────────────┴────────────────┴────────────────┴─────────┴────────────────┘
┌─step─┬─────type─┬─────────────amount─┬─nameOrig────┬─oldBalanceOrg─┬──────newBalanceOrg─┬─nameDest────┬─────oldBalanceDest─┬─────newBalanceDest─┬─isFraud─┬─isFlaggedFraud─┐
│  186 │  CASH_IN │ 43869.259999999995 │ C499863103  │    3263326.14 │          3307195.4 │ C1146577120 │  7243599.649999999 │         7199730.39 │       0 │              0 │
│  188 │  CASH_IN │          105290.69 │ C2050755239 │    8970829.91 │          9076120.6 │ C1146577120 │         7199730.39 │          7094439.7 │       0 │              0 │
│  203 │  CASH_IN │ 152809.84999999998 │ C306906600  │             0 │ 152809.84999999998 │ C1146577120 │          7094439.7 │         6941629.85 │       0 │              0 │
│  204 │  CASH_IN │          404606.06 │ C298966652  │         11011 │          415617.06 │ C1146577120 │         6941629.85 │         6537023.79 │       0 │              0 │
│  215 │ TRANSFER │          945430.48 │ C445082147  │          7725 │                  0 │ C1146577120 │         6537023.79 │ 7482454.2700000005 │       0 │              0 │
│  226 │ CASH_OUT │ 64991.380000000005 │ C841892048  │         16735 │                  0 │ C1146577120 │ 7482454.2700000005 │         8547117.15 │       0 │              0 │
│  226 │ TRANSFER │           999671.5 │ C2020422720 │             0 │                  0 │ C1146577120 │  7547445.649999999 │         8547117.15 │       0 │              0 │
│  229 │ CASH_OUT │           15851.58 │ C1726476985 │      21594.53 │            5742.95 │ C1146577120 │         8547117.15 │  8562968.729999999 │       0 │              0 │
└──────┴──────────┴────────────────────┴─────────────┴───────────────┴────────────────────┴─────────────┴────────────────────┴────────────────────┴─────────┴────────────────┘

50 rows in set. Elapsed: 0.046 sec. Processed 6.36 million rows, 572.41 MB (137.52 million rows/s., 12.37 GB/s.)
```


