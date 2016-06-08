*欢迎使用本SDK*

*模块安装方式*

    pip install shata-messageclient -i http://pypi.shatacloud.com/ci/dev --trusted-host pypi.shatacloud.com
    
*模块使用指南*


*多客户端rpc调用*

### 服务端实现
    from messageclient import RpcConsumer
    from oslo_config import cfg

    conf = cfg.CONF
    rabbit_opts = [
        cfg.StrOpt('mq_hosts', default='172.30.40.246'),
        cfg.PortOpt('mq_port', default=5672),
        cfg.StrOpt('mq_username', default='guest'),
        cfg.StrOpt('mq_password', default='guest'),
        cfg.StrOpt('mq_virtual_host', default='/'),
        cfg.IntOpt('mq_heartbeat_interval', default=2)
    ]

    conf.register_opts(rabbit_opts)
    conf(project='iaas')  # load config file /etc/iaas/iaas.conf

    class TestConsumer(RpcConsumer):
        def __init__(self, conf, queue):
            super(TestConsumer, self).__init__(conf, queue)
        
        def handle_message(self, message):
            print 'Receive Message: %s' % message

    if __name__ == '__main__':
        consumer = TestConsumer(conf, 'rpc')

### 客户端实现
    from messageclient import PpcPublisher
    from oslo_config import cfg

    conf = cfg.CONF
    rabbit_opts = [
        cfg.StrOpt('mq_hosts', default='172.30.40.246'),
        cfg.PortOpt('mq_port', default=5672),
        cfg.StrOpt('mq_username', default='guest'),
        cfg.StrOpt('mq_password', default='guest'),
        cfg.StrOpt('mq_virtual_host', default='/'),
        cfg.IntOpt('mq_heartbeat_interval', default=2)
    ]

    conf.register_opts(rabbit_opts)
    conf(project='iaas')  # load config file /etc/iaas/iaas.conf
        
    if __name__ == '__main__':
        rpc = RpcPublisher(conf, 'rpc', 'rpc-callback-1')
        result = rpc.send_message({'hello': 'world'})
        print result
            
            
            
            

*阻塞方式调用*

### 发送消息

    import messageclient
    
    transport = messageclient.get_transport(conf)
    target = messageclient.Target(queue='IaasService')
    message = messageclient.Message(transport, target, msg_body)
    messageclient.send_message(message, mode='rpc')
    

### 接收处理消息
    
    import messageclient
    
    transport = messageclient.get_transport(conf)
    target = messageclient.Target(queue='IaasService')
    messageclient.start_consume_message(transport, target, callback)


### 异步消息发送处理
    import messageclient

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
        'network': ['ext-net', 'int-net']
        'key_name': 'dev',
        'tenant_name': 'dev'
    }

    @messageclient.on_response
    def on_response(message):
    """
    handle result returned by send_request method.
    :param message: dict, user mesage
    """
        print 'receive message: %s' % message

    transport = messageclient.get_transport(CONF)
    target = messageclient.Target(queue='IaasService')
    message = messageclient.Message(transport, target, msg_body)
    messageclient.send_request(message)
    messagecient.receive_response(transport, target, on_response)      # non-blocking call, return immediately

    # ... main thread handle

### example client

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
    target = messageclient.Target(queue='iaas')
    message = messageclient.Message(transport, target, msg_body)
    result = messageclient.send_message(message, mode='rpc')
    print result
    

### example server

    import messageclient
    import json
    
    class CONF:
        mq_hosts = '172.30.40.246'
        mq_port = 5672
        mq_username = 'guest'
        mq_password = 'guest'
        mq_virtual_host = '/'
        mq_heartbeat_interval = 2
    
    @messageclient.on_message    
    def on_message(message):
        print 'receive message: ', info
        result = {'ip': '172.30.40.201', 'user': 'cloud', 'password': '123456'}
        return result

    transport = messageclient.get_transport(CONF)
    target = messageclient.Target(queue='iaas')
    messageclient.start_consume_message(transport, target, on_message)
    



