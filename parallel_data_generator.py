from numpy.random import choice, randint, default_rng
import uuid
from datetime import datetime, timezone
from tqdm import tqdm
import pandas as pd
from multiprocessing import Pool
import marshal
from types import FunctionType

MICROSERVICE_ID = ['e8372c50-1678-4987-8105-966238974c4e',
'ef7119e1-d772-421b-99c1-c3cb3105ace9',
'ac727270-0142-4b09-a487-de57a9bf803d', '29498e6f-adc2-4830-8b53-ffaf9a6ed20c']
EVENT_TYPE = ['VIEW', 'TRANSACTION', 'CPU', 'RAM']
operation_type = ['VIEW', 'BUY', 'CANCEL', 'REFUND']
LEVEL = ['TRACE', 'DEBUG', 'ERROR']
OPERATION_TYPE = operation_type
OPERATION_TYPE.append('GET')
OPERATION_TYPE.append('POST')
simultaneous_users = 12
average_time_in_site = 6 #sec
potencial_users = 1000

potential_operation_id = 100
simultaneous_operation_id = 3

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

user_db = []
for user_id in range(potencial_users):
    user_db.append(str(uuid.uuid4()))

def common_metrics_logs_per_one(user):
    user_metrics = []
    user_logs = []
    action_ids = [str(uuid.uuid4()) for _ in range (simultaneous_operation_id)]
    # user = choice(user_db)
    for _ in range(randint(1, 30)):
        dt_now=datetime.now().replace(tzinfo=timezone.utc).timestamp()
        
        logs = {}
        logs['timestamp'] = dt_now
        logs['level'] = choice(LEVEL)
        logs['microservice_id'] = choice(MICROSERVICE_ID)
        logs['operation_type'] = choice(operation_type)
        logs['action_id'] = choice(action_ids)
        logs['user_id'] = user

        metrics = {}
        metrics['timestamp'] = dt_now
        metrics['microservice_id'] = choice(MICROSERVICE_ID)
        metrics['operation_type'] = choice(OPERATION_TYPE)
        metrics['action_id'] = choice(action_ids)
        metrics['user_id'] = user
        metrics['value'] = randint(1, 10)
        
        user_logs.append(logs)
        user_metrics.append(metrics)
    return [user_metrics, user_logs]


def dos_metrics_logs_per_one(user):
    user_metrics = []
    user_logs = []
    action_ids = [str(uuid.uuid4()) for _ in range (simultaneous_operation_id)]
    # user = choice(user_db)
    for _ in range(randint(300, 600)):
        dt_now=datetime.now().replace(tzinfo=timezone.utc).timestamp()
        
        logs = {}
        logs['timestamp'] = dt_now
        logs['level'] = choice(LEVEL)
        logs['microservice_id'] = choice(MICROSERVICE_ID)
        logs['operation_type'] = choice(operation_type)
        logs['action_id'] = choice(action_ids)
        logs['user_id'] = user

        metrics = {}
        metrics['timestamp'] = dt_now
        metrics['microservice_id'] = choice(MICROSERVICE_ID)
        metrics['operation_type'] = choice(OPERATION_TYPE)
        metrics['action_id'] = choice(action_ids)
        metrics['user_id'] = user
        metrics['value'] = randint(1, 10)
        
        user_logs.append(logs)
        user_metrics.append(metrics)
    return [user_metrics, user_logs]

if __name__ == "__main__":
    metrics = []
    logs = []
    pool    = Pool(processes=simultaneous_users)
    for _ in tqdm(range(1000)):
        random = default_rng().uniform(size=1)
        if random < 0.98:
            users = choice(user_db, 50)
            result = [pool.apply_async(*make_applicable(common_metrics_logs_per_one, user)) for user in users]
        elif (random >= 0.98) and (random < 0.995): #dos attack
            users = choice(user_db, 1)
            result = [pool.apply_async(*make_applicable(dos_metrics_logs_per_one, user)) for user in users]
            print('generate dos attack. Random=', random)
        elif random >= 0.995: #ddos attack
            users = choice(user_db, 400)
            result = [pool.apply_async(*make_applicable(dos_metrics_logs_per_one, user)) for user in users]
            print('generate ddos attack. Random=', random)

        for r in result:
            metrics.append(r.get()[0])
            logs.append(r.get()[1])
            # print('Metrics \t', r.get()[0])
            # print('Logs \t', r.get()[1])

    metrics = pd.DataFrame(metrics)
    logs = pd.DataFrame(logs)
    metrics.to_csv('metrics.csv')
    logs.to_csv('logs.csv')

