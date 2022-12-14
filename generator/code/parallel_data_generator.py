from numpy.random import choice, randint, default_rng
import numpy as np
import uuid
from datetime import datetime, timezone
from tqdm import tqdm
from multiprocessing import Pool
import marshal
from types import FunctionType
import time
import multiprocessing
import string
import random as rnd

MICROSERVICE_ID = ['e8372c50-1678-4987-8105-966238974c4e',
                   'ef7119e1-d772-421b-99c1-c3cb3105ace9',
                   'ac727270-0142-4b09-a487-de57a9bf803d', '29498e6f-adc2-4830-8b53-ffaf9a6ed20c']
EVENT_TYPE = ['VIEW', 'TRANSACTION', 'BUY', 'CANCEL', 'REFUND']
LEVEL = ['TRACE', 'DEBUG', 'ERROR']
OPERATION_TYPE = ['VIEW', 'BUY', 'CANCEL', 'REFUND']
# OPERATION_TYPE.append('GET')
# OPERATION_TYPE.append('POST')
simultaneous_users = 12
average_time_in_site = 6  # sec
potencial_users = 10000

potential_operation_id = 100
simultaneous_operation_id = 3


def _applicable(*args, **kwargs):
    name = kwargs['__pw_name']
    code = marshal.loads(kwargs['__pw_code'])
    gbls = globals()  # gbls = marshal.loads(kwargs['__pw_gbls'])
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
    kwargs['__pw_code'] = marshal.dumps(f.__code__)  # edited
    kwargs['__pw_defs'] = marshal.dumps(f.__defaults__)  # edited
    kwargs['__pw_clsr'] = marshal.dumps(f.__closure__)  # edited
    kwargs['__pw_fdct'] = marshal.dumps(f.__dict__)  # edited
    return _applicable, args, kwargs


def _mappable(x):
    x, name, code, defs, clsr, fdct = x
    code = marshal.loads(code)
    gbls = globals()  # gbls = marshal.loads(gbls)
    defs = marshal.loads(defs)
    clsr = marshal.loads(clsr)
    fdct = marshal.loads(fdct)
    func = FunctionType(code, gbls, name, defs, clsr)
    func.fdct = fdct
    return func(x)


def make_mappable(f, iterable):
    if not isinstance(f, FunctionType): raise ValueError('argument must be a function')
    name = f.__name__  # edited
    code = marshal.dumps(f.__code__)  # edited
    defs = marshal.dumps(f.__defaults__)  # edited
    clsr = marshal.dumps(f.__closure__)  # edited
    fdct = marshal.dumps(f.__dict__)  # edited
    return _mappable, ((i, name, code, defs, clsr, fdct) for i in iterable)


user_db = []
for _ in range(potencial_users):
    user_db.append(str(uuid.uuid4()))


def user_ddos(number=1):
    ddos_user = []
    for _ in range(number):
        ddos_user.append(str(uuid.uuid4()))
    return ddos_user


def common_metrics_logs_per_one(user):
    user_metrics = []
    user_logs = []
    action_id = str(uuid.uuid4())
    # user = choice(user_db)
    randomActionNum = rnd.normalvariate(mu=13, sigma=8)
    randomActionInt = int(np.abs(np.round(randomActionNum)))
    # print(randomActionInt)
    for _ in range(randomActionInt):
        timeBetweenActions = np.abs(rnd.normalvariate(mu=0.45, sigma=0.3)) # 0.4 is considered avg time between actions
        # print(timeBetweenActions)
        time.sleep(timeBetweenActions)
        dt_now = datetime.now().replace(tzinfo=timezone.utc).timestamp()
        tmp_hueta = datetime.fromtimestamp(dt_now).strftime('%Y-%m-%d %H:%M:%S')
        random = default_rng().uniform(size=1)

        logs = {}
        logs['id'] = str(uuid.uuid1())
        logs['timestamp'] = tmp_hueta
        logs['level'] = choice(LEVEL)
        microservice_id = choice(MICROSERVICE_ID)
        logs['microservice_id'] = microservice_id
        logs['operation_type'] = choice(OPERATION_TYPE)
        logs['action_id'] = action_id
        logs['user_id'] = user
        logs['message'] = ''.join(rnd.choices(string.ascii_uppercase + string.digits, k=6))

        metrics = {}
        metrics['id'] = str(uuid.uuid1())
        metrics['timestamp'] = tmp_hueta
        metrics['microservice_id'] = microservice_id
        metrics['operation_type'] = choice(EVENT_TYPE)
        metrics['action_id'] = action_id
        metrics['user_id'] = user
        metrics['value'] = 0

        user_logs.append(logs)
        user_metrics.append(metrics)

        if random > 0.99:
            max_retire_count = 7
            for i in range(max_retire_count):
                time.sleep(0.5)
                dt_now = datetime.now().replace(tzinfo=timezone.utc).timestamp()
                tmp_hueta = datetime.fromtimestamp(dt_now).strftime('%Y-%m-%d %H:%M:%S')
                logs = {}
                logs['id'] = str(uuid.uuid1())
                logs['timestamp'] = tmp_hueta
                logs['level'] = choice(LEVEL)
                logs['microservice_id'] = microservice_id
                logs['operation_type'] = choice(OPERATION_TYPE)
                logs['action_id'] = action_id
                logs['user_id'] = user
                logs['message'] = ''.join(rnd.choices(string.ascii_uppercase + string.digits, k=6))

                metrics = {}
                metrics['id'] = str(uuid.uuid1())
                metrics['timestamp'] = tmp_hueta
                metrics['microservice_id'] = microservice_id
                metrics['operation_type'] = choice(EVENT_TYPE)
                metrics['action_id'] = action_id
                metrics['user_id'] = user
                if default_rng().uniform(size=1) > 0.7:
                    metrics['value'] = -1
                    user_logs.append(logs)
                    user_metrics.append(metrics)
                else:
                    metrics['value'] = 1
                    user_logs.append(logs)
                    user_metrics.append(metrics)
                    break

    return [user_metrics, user_logs]


def dos_metrics_logs_per_one(user):
    user_metrics = []
    user_logs = []
    action_id = str(uuid.uuid4())
    for _ in range(randint(300, 600)):
        dt_now = datetime.now().replace(tzinfo=timezone.utc).timestamp()
        tmp_hueta = datetime.fromtimestamp(dt_now).strftime('%Y-%m-%d %H:%M:%S')

        logs = {}
        logs['id'] = str(uuid.uuid1())
        logs['timestamp'] = tmp_hueta
        logs['level'] = choice(LEVEL)
        logs['microservice_id'] = choice(MICROSERVICE_ID)
        logs['operation_type'] = choice(OPERATION_TYPE)
        logs['action_id'] = action_id
        logs['user_id'] = user
        logs['message'] = ''.join(rnd.choices(string.ascii_uppercase + string.digits, k=6))

        metrics = {}
        metrics['id'] = str(uuid.uuid1())
        metrics['timestamp'] = tmp_hueta
        metrics['microservice_id'] = choice(MICROSERVICE_ID)
        metrics['operation_type'] = choice(EVENT_TYPE)
        metrics['action_id'] = action_id
        metrics['user_id'] = user
        metrics['value'] = randint(1, 2)

        user_logs.append(logs)
        user_metrics.append(metrics)
    return [user_metrics, user_logs]


def resources():
    resource = []
    dt_now = datetime.now().replace(tzinfo=timezone.utc).timestamp()
    tmp_hueta = datetime.fromtimestamp(dt_now).strftime('%Y-%m-%d %H:%M:%S')
    for i in MICROSERVICE_ID:
        tmp = {}
        tmp['id'] = str(uuid.uuid1())
        tmp['timestamp'] = tmp_hueta
        tmp['microservice_id'] = i
        tmp['cpu'] = randint(50, 100)
        tmp['ram'] = randint(500, 8000)
        resource.append(tmp)
    return resource


def metrics_logs_generator():
    metrics = []
    logs = []
    pool = Pool(processes=multiprocessing.cpu_count())
    for _ in tqdm(range(3)):
        random = default_rng().uniform(size=1)
        if random < 0.99:
            users = choice(user_db, 50)
            result = [pool.apply_async(*make_applicable(common_metrics_logs_per_one, user)) for user in users]
        elif (random >= 0.99) and (random < 0.997):  # dos attack
            pool = Pool(processes=1)
            users = user_ddos()
            result = [pool.apply_async(*make_applicable(dos_metrics_logs_per_one, user)) for user in users]
            print('generate dos attack. Random=', random)
            pool = Pool(processes=simultaneous_users)
        elif random >= 0.997:  # ddos attack
            users = user_ddos(randint(300, 400))
            result = [pool.apply_async(*make_applicable(dos_metrics_logs_per_one, user)) for user in users]
            print('generate ddos attack. Random=', random)

        for r in result:
            metrics.append(r.get()[0])
            logs.append(r.get()[1])
    return metrics, logs

# def serialize(data):
#     for r in data:
#         for k in r:
#             yield (json.dumps(k), "utf-8")

# if __name__ == "__main__":
#     series = serialize(metrics_logs_generator()[0])
#     for ss in series:
#         print(ss)
