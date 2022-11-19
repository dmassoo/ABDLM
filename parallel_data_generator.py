import pandas as pd
from numpy.random import choice, randint
import uuid
from datetime import datetime, timezone, timedelta
from tqdm import tqdm
import concurrent.futures
from joblib import Parallel, delayed
import multiprocessing
from multiprocessing import Pool
from types import FunctionType
import marshal
from multiprocessing import Pool


EVENT_TYPE = ['VIEW', 'TRANSACTION', 'CPU', 'RAM']
MICROSERVICE_ID = ['VIEW', 'BUY', 'CANCEL', 'REFUND']
LEVEL = ['TRACE', 'DEBUG', 'ERROR']
OPERATION_TYPE = MICROSERVICE_ID
OPERATION_TYPE.append('GET')
OPERATION_TYPE.append('POST')

simultaneous_users = 12
average_time_in_site = 6 #sec
potencial_users = 1000

potential_operation_id = 100
simultaneous_operation_id = 3

user_db = []
for user_id in range(potencial_users):
    user_db.append(str(uuid.uuid4()))

def common_metrics_per_one(user):
    user_metrics = []
    operation_ids = [str(uuid.uuid4()) for _ in range (3)]
    # user = choice(user_db)
    for i in range(randint(1, 30)):
        metrics = {}
        dt_now=datetime.now().replace(tzinfo=timezone.utc).timestamp()
        metrics['timestamp'] = dt_now
        metrics['microservice_id'] = choice(MICROSERVICE_ID)
        metrics['operation_id'] = choice(operation_ids)
        metrics['user_id'] = user
        metrics['event_type'] = choice(EVENT_TYPE)

        def generate_metrics_value():
            return 1

        metrics['value'] = generate_metrics_value()
        user_metrics.append(metrics)
    return user_metrics

def driver_func():
    PROCESSES = 5
    with multiprocessing.Pool(PROCESSES) as pool:
        users = choice(user_db, 50)
        results = [pool.apply_async(common_metrics_per_one, user) for user in users]
    return results

def _applicable(*args, **kwargs):
    name = kwargs['__pw_name']
    code = marshal.loads(kwargs['__pw_code'])
    gbls = globals() #gbls = marshal.loads(kwargs['__pw_gbls'])
    defs = marshal.loads(kwargs['__pw_defs'])
    clsr = marshal.loads(kwargs['__pw_clsr'])
    fdct = marshal.loads(kwargs['__pw_fdct'])
    func = FunctionType(code, gbls, name, defs, clsr)
    func.fdct = fdct
    del kwargs['__pw_name']
    del kwargs['__pw_code']
    del kwargs['__pw_defs']
    del kwargs['__pw_clsr']
    del kwargs['__pw_fdct']
    return func(*args, **kwargs)

def make_applicable(f, *args, **kwargs):
    if not isinstance(f, FunctionType): raise ValueError('argument must be a function')
    kwargs['__pw_name'] = f.__name__  # edited
    kwargs['__pw_code'] = marshal.dumps(f.__code__)   # edited
    kwargs['__pw_defs'] = marshal.dumps(f.__defaults__)  # edited
    kwargs['__pw_clsr'] = marshal.dumps(f.__closure__)  # edited
    kwargs['__pw_fdct'] = marshal.dumps(f.__dict__)   # edited
    return _applicable, args, kwargs

def _mappable(x):
    x,name,code,defs,clsr,fdct = x
    code = marshal.loads(code)
    gbls = globals() #gbls = marshal.loads(gbls)
    defs = marshal.loads(defs)
    clsr = marshal.loads(clsr)
    fdct = marshal.loads(fdct)
    func = FunctionType(code, gbls, name, defs, clsr)
    func.fdct = fdct
    return func(x)

def make_mappable(f, iterable):
    if not isinstance(f, FunctionType): raise ValueError('argument must be a function')
    name = f.__name__    # edited
    code = marshal.dumps(f.__code__)   # edited
    defs = marshal.dumps(f.__defaults__)  # edited
    clsr = marshal.dumps(f.__closure__)  # edited
    fdct = marshal.dumps(f.__dict__)  # edited
    return _mappable, ((i,name,code,defs,clsr,fdct) for i in iterable)

if __name__ == "__main__":
    data = []
    users = choice(user_db, 10)
    pool    = Pool(processes=2)
    results = [pool.apply_async(*make_applicable(common_metrics_per_one, user)) for user in users]
    for r in tqdm(results):
        # data.append(r.get())
        print('\t', r.get())