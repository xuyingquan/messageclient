import messageclient

class CONF:
    hosts = '172.30.40.246'
    port = 5672
    username = 'guest'
    password = 'guest'
    virtual_host = '/'
    heartbeat_interval = 2

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
