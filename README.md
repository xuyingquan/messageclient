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

### 发送消息（客户端）

    import messageclient
    
    transport = messageclient.get_transport(conf)
    target = messageclient.Target(queue='IaasService')
    message = messageclient.Message(transport, target, header={'type': 'test'}, body={})
    result = messageclient.send_message(message)
    

### 接收处理消息（服务端）
    
    import messageclient
    
    @messageclient.on_message(type='test')
    def on_message(message):
        print 'receive message: ', message
        result = {'ip': '172.30.40.201', 'user': 'cloud', 'password': '123456'}
        return result
    
    transport = messageclient.get_transport(conf)
    target = messageclient.Target(queue='IaasService')
    messageclient.start_consume_message(transport, target)


### 异步消息发送处理 (客户端)

    import messageclient

    @messageclient.on_message(type='test')
    def on_response(message):
        """ 处理异步返回结果
        :param message: dict, 用户定义的消息结构
        
        """
        print 'receive message: %s' % message

    transport = messageclient.get_transport(conf)
    target = messageclient.Target(queue='IaasService')
    message = messageclient.Message(transport, target, header={'type': 'test'}, body={})
    messageclient.send_request(message)                     # 或者 messageclient.send_message(message, mode='async')
    messagecient.receive_response(transport, target)        # non-blocking call, return immediately

    # ... main thread handle



