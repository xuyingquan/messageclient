import messageclient
import json
from messageclient import LOG

class CONF:
    mq_hosts = '172.30.40.246'
    mq_port = 5672
    mq_username = 'guest'
    mq_password = 'guest'
    mq_virtual_host = '/'
    mq_heartbeat_interval = 2

"""
def on_message(ch, method, props, body):
    info = json.loads(body)
    print 'receive message: ', info
    result = {'ip': '172.30.40.201', 'user': 'cloud', 'password': '123456'}
    messageclient.send_rpc_response(ch, method, props, result)
"""


@messageclient.on_message
def on_message(message):
    print 'receive message: ', message
    result = {'ip': '172.30.40.201', 'user': 'cloud', 'password': '123456'}
    return result


transport = messageclient.get_transport(CONF)
target = messageclient.Target(queue='IaasService', broadcast=False)  # receive broadcast notification.
messageclient.start_consume_message(transport, target, on_message)
LOG.info('hello world')
