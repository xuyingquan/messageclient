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
@messageclient.on_message(type='test')
def on_message(message):
    print 'receive message: ', message
    result = {'ip': '172.30.40.201', 'user': 'cloud', 'password': '123456'}
    return result


transport = messageclient.get_transport(CONF)
target = messageclient.Target(queue='IaasService', broadcast=False)  # receive broadcast notification.
messageclient.start_consume_message(transport, target)
LOG.info('hello world')
"""


class TestConsumer(messageclient.Consumer):
    def __init__(self, conf, queue):
        super(TestConsumer, self).__init__(conf, queue)

    @messageclient.on_message_v1(type='test')
    def on_message_test(self, message):
        print 'receive message: ', message
        result = {'ip': '172.30.40.201', 'user': 'cloud', 'password': '123456'}
        return result


consumer = TestConsumer(CONF, 'IaasService')
