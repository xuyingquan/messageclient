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
    'tenant_name': 'dev'
}


@messageclient.on_response
def on_response(message):
    print 'receive message: %s' % message

transport = messageclient.get_transport(CONF)
target = messageclient.Target(queue='IaasService')
message = messageclient.Message(transport, target, msg_body)
messageclient.send_request(message)
messageclient.receive_response(on_response)
while True:
    print 'execute main thead task.'
    time.sleep(10)
