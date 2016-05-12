调用方接口：

    def send_message(target, message):
        """
        :param target: dict
        :param message: user msg.
        :return: message response result.
        """
        pass
    
服务方接口：

    def start_listen_message(url, queue, callback):
        """
        :param url: dict, parameter info to connect rabbitmq.
        :param queue: the queue name you will listening.
        :param callback: your message handler.
        :return: None
        pass
