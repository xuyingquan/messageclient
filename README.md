*欢迎使用本SDK*

*模块安装方式*

    pip install shata-messageclient -i http://pypi.shatacloud.com/ci/dev --trusted-host pypi.shatacloud.com
    
*模块使用指南*


*类接口*

### 服务端实现
    import messageclient
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

    class DevopsConsumer(messageclient.Consumer):
        def __init__(self, conf, queue):
            super(DevopsConsumer, self).__init__(conf, queue)
        
        @messageclient.on_message_v1(type='cd_service')
        def handle_message_cd(self, message):
            """ 处理cd_service类型的消息
            
            """
            print 'Receive Message: %s' % message
            return dict(result='ok')
           
        @messageclient.on_message_v1(type='iaas_service')
        def handle_message_iaas(self, message):
            """ 处理iaas_service类型的消息
            
            """
            print 'Receive Message: %s' % message
            return dict(ip='192.168.1.10', user='cloud', paasword='123456')
        
        @messageclient.on_message_v1(type='biz_service')
        def handle_message_biz(self, message):
            """ 处理biz_service类型的消息
            
            """
            print 'Receive Message: %s' % message
            return dict(result='ok')

    if __name__ == '__main__':
        consumer1 = DevopsConsumer(conf, 'iaas_service')
        consumer2 = DevopsConsumer(conf, 'cd_service')
        consumer3 = DevopsConsumer(conf, 'biz_service')

### 客户端实现
    from messageclient import RpcPublisher
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
        publisher = RpcPublisher(conf)       # 全局对象
        
        # 用户产生的消息对象
        message = messageclient.Message(header={'type': 'iaas_service'}, body={'hello': 'world'})
        
        # 同步发送消息，等待返回结果
        result = publisher.send_message(message, queue='iaas_service', reply_queue='reply-iaas_service')
        print result
        
        # 异步发送消息，函数立即返回，单纯发送消息，没有返回结果
        publisher.send_request(message, queue='iaas_service')
        
        # 广播消息
        publisher.broadcast_message(message, queues=['iaas_service', 'cd_service', 'biz_service'])
        
        # 异步发送消息，函数立即返回，有返回结果
        publisher.send_request(message, queue='iaas_service', reply_queue='reply-iaas_service')
        DevopsConsumer(conf, queue='reply-iaas_service')    # 处理消息返回结果
            
            
            
            

*阻塞方式调用*

### 发送消息（客户端）

    import messageclient
    
    transport = messageclient.get_transport(conf)
    target = messageclient.Target(queue='IaasService')
    message = messageclient.Message(header={'type': 'test'}, body={})
    result = transport.send_message(target, message, callback_queue='test')
    

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
    message = messageclient.Message(header={'type': 'test'}, body={})
    transport.send_request(target, message, reply_queue='IaasService-Reply-1')
    transport.receive_response(target)        # non-blocking call, return immediately

    # ... main thread handle


