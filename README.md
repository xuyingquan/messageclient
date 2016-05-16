*欢迎使用本SDK*

*模块安装方式*

    pip install shata-messageclient -i http://pypi.shatacloud.com/ci/dev --trusted-host pypi.shatacloud.com
    
*模块使用指南*


### 发送消息

    import messageclient
    
    transport = messageclient.get_transport(conf)
    target = messageclient.Target(appname='IaasServic')
    message = messageclient.Message(transport, target, msg_body)
    messageclient.send_message(message, mode='rpc')
    

### 接收处理消息
    
    import messageclient
    
    transport = messageclient.get_transport(conf)
    target = messageclient.Target(appname='IaasService')
    messageclient.start_consume_message(transport, target, callback)
    

### example client

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
    target = messageclient.Target(appname='iaas', topic='')
    message = messageclient.Message(transport, target, msg_body)
    result = messageclient.send_message(message, mode='rpc')
    print result
    

### example server

    import messageclient
    import json
    
    class CONF:
        hosts = '172.30.40.246'
        port = 5672
        username = 'guest'
        password = 'guest'
        virtual_host = '/'
        heartbeat_interval = 2
    
    
    def on_message(ch, method, props, body):
        info = json.loads(body)
        print 'receive message: ', info
        result = {'ip': '172.30.40.201', 'user': 'cloud', 'password': '123456'}
        messageclient.send_rpc_response(ch, method, props, result)
    
    transport = messageclient.get_transport(CONF)
    target = messageclient.Target(appname='iaas', topic='')
    messageclient.start_consume_message(transport, target, on_message)
