from template.db import Database
from template.query import Query
from template.transaction import Transaction
from template.transaction_worker import TransactionWorker
from template.config import init
import time

from random import choice, randint, sample, seed

init()
db = Database()
db.open('./ECS165')
grades_table = db.create_table('Grades', 5, 0)

keys = []
records = {}
seed(3562901)
num_threads = 8

try:
    grades_table.index.create_index(1)
    grades_table.index.create_index(2)
    grades_table.index.create_index(3)
    grades_table.index.create_index(4)
except Exception as e:
    print('Index API not implemented properly, tests may fail.')

# Insert Transaction Workers
transaction_workers_insert = []
insert_transactions = []
for i in range(num_threads):
    insert_transactions.append(Transaction())
    transaction_workers_insert.append(TransactionWorker())
    transaction_workers_insert[i].add_transaction(insert_transactions[i])
worker_keys = [ {} for t in transaction_workers_insert ]

for i in range(0, 1000):
    key = 92106429 + i
    keys.append(key)
    i = i % num_threads
    records[key] = [key, randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20)]
    #records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
    q = Query(grades_table)
    insert_transactions[i].add_query(q.insert, *records[key])
    worker_keys[i][key] = True

# Running insertion transaction workers
transaction_worker_counter = 0
for transaction_worker in transaction_workers_insert:
    transaction_worker.run()
    print("starting insert transaction worker ", transaction_worker_counter)
    transaction_worker_counter += 1
    # transaction_worker.thread.join()

transaction_workers_select = []
select_transactions = []
for i in range(num_threads):
    select_transactions.append(Transaction())
    transaction_workers_select.append(TransactionWorker())
    transaction_workers_select[i].add_transaction(select_transactions[i])

t = 0
_records = [records[key] for key in keys]
for c in range(grades_table.num_columns):
    _keys = sorted(list(set([record[c] for record in _records])))
    index = {v: [record for record in _records if record[c] == v] for v in _keys}
    for key in _keys:
        found = True
        for record in index[key]:
            if record[0] not in worker_keys[t % num_threads]:
                found = False
        if found:
            query = Query(grades_table)
            select_transactions[t % num_threads].add_query(query.select, key, c, [1, 1, 1, 1, 1])
        t += 1

transaction_workers_update = []
update_transactions = []
for i in range(num_threads):
    update_transactions.append(Transaction())
    transaction_workers_update.append(TransactionWorker())
    transaction_workers_update[i].add_transaction(update_transactions[i])

for j in range(0, num_threads):
    for key in worker_keys[j]:
        updated_columns = [None, None, None, None, None]
        for i in range(1, grades_table.num_columns):
            value = randint(0, 20)
            updated_columns[i] = value
            records[key][i] = value
            query = Query(grades_table)
            update_transactions[j].add_query(query.update, key, *updated_columns)
            updated_columns = [None, None, None, None, None]

transaction_worker_counter = 0
for transaction_worker in transaction_workers_update:
    transaction_worker.run()
    print("starting insert transaction worker ", transaction_worker_counter)
    transaction_worker_counter += 1
    # transaction_worker.thread.join()

time.sleep(30)

score = len(keys)
for key in keys:
    correct = records[key]
    query = Query(grades_table)
    #TODO: modify this line based on what your SELECT returns
    result = query.select(key, 0, [1, 1, 1, 1, 1])[0].columns
    if correct != result:
        print('select error on primary key', key, ':', result, ', correct:', correct)
        score -= 1
print('Score', score, '/', len(keys))

db.close()