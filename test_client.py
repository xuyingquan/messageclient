import messageclient

class CONF:
    mq_hosts = '172.30.40.246'
    mq_port = 5672
    mq_username = 'guest'
    mq_password = 'guest'
    mq_virtual_host = '/'
    mq_heartbeat_interval = 2

msg_body = {
    'cpu': 2,
    'mem': 4096,
    'disk': 40,
    'os': {'type': 'ubuntu', 'version': '14.04'}
}
transport = messageclient.get_transport(CONF)
target = messageclient.Target(queue='IaasService')
message = messageclient.Message(transport, target, msg_body)
result = messageclient.send_message(message, mode='rpc')
print result
