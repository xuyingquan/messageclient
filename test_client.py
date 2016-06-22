# -*- coding: utf-8 -*-

import messageclient
import time

class CONF:
    mq_hosts = '172.30.40.246'
    mq_port = 5672
    mq_username = 'guest'
    mq_password = 'guest'
    mq_virtual_host = '/'
    mq_heartbeat_interval = 2

msg_body = {
    'action': 'acquire',
    'name': 'test',
    'flavor': {
        'cpu': 2,
        'mem': 4096,
        'disk': 40
    },
    'image': {
        'os_type': 'ubuntu',
        'os_version': '14.04',
        'os_arch': 'x86_64'
    },
    'network': ['ext-net', 'int-net'],
    'key_name': 'dev',
    'tenant_name': 'admin'
}

"""
@messageclient.on_message(type='test')
def on_response(message):
    print 'receive message: %s' % message
"""

def main():
    transport = messageclient.get_transport(CONF)
    target = messageclient.Target(queue='IaasService')
    message = messageclient.Message(header={'type': 'test'}, body=msg_body)

    result = None

    if test_method == 'sync':
        # 测试阻塞发送消息
        result = transport.send_message(target, message, callback_queue="IaasService-reply")
        #target = messageclient.Target(queue='test')
        #print transport.send_message(target, message, callback_queue='xyq-callback-1')
    elif test_method == 'async':
        # 测试异步发送消息
        transport.send_request(target, message)
        transport.receive_response(target)
    else:
        pass

    print result


class TestConsumer(messageclient.Consumer):
    def __init__(self, conf, queue):
        super(TestConsumer, self).__init__(conf, queue)

    @messageclient.on_message_v1(type='iaas_service')
    def handle_message(self, message):
        print 'receive response: %s' % message
        return dict(ip='192.168.1.10', user='cloud', password='123456')


if __name__ == '__main__':
    test_method = 'sync'
    # main()
    from messageclient import RpcPublisher
    publisher = RpcPublisher(CONF)
    message = messageclient.Message(header={'type': 'iaas_service'}, body=msg_body)
    # print publisher.send_message(message, queue='iaas_service', reply_queue='reply-iaas_service')
    # print publisher.send_message(message, queue='cd_service', reply_queue='reply-cd_service')
    # print publisher.send_message(message, queue='biz_service', reply_queue='reply-biz_service')
    # publisher.broadcast_message(message, queues=['cd_service', 'iaas_service', 'biz_service'])

    publisher.send_request(message, queue='iaas_service', reply_queue='reply-iaas_service')

    TestConsumer(CONF, 'reply-iaas_service')

