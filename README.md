*欢迎使用本SDK*

*模块安装方式*

    pip install shata-messageclinet -i http://pypi.shatacloud.com/ci/dev --trusted-host pypi.shatacloud.com
    
*模块使用指南*


### 发送消息

    import messageclient
    
    transport = messageclient.get_transport(conf)
    target = messageclient.Target(**kwargs)
    messageclient.send_message(transport, target, message)
    

### 接收处理消息
    
    import messageclient
    
    transport = messageclient.get_transport(conf)
    target = messageclient.Target(**kwargs)
    messageclient.start_consume_message(transport, target, callback)