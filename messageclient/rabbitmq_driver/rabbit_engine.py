#!/usr/bin/python
# -*- coding: utf-8 -*-
#########################################################################
# File Name: rabbit_engine.py
# Author: xuyingquan
# mail: yingquan.xu@shatacloud
# Created Time: Thu May 12 11:21:19 HKT 2016
#########################################################################

import pika
import threading
import random
import traceback
import json
import uuid
import messageclient

from messageclient import LOG


class PikaEngine(object):
    """
    Used for shared functionality between other pika driver modules, like
    connection factory, connection pools, processing and holding configuration,
    etc.
    """
    def __init__(self, conf, default_exchange=None):
        self.conf = conf
        self._common_pika_params = {
            'virtual_host': '/',
        }
        if self.conf.mq_heartbeat_interval:
            self._heartbeat_interval = self.conf.mq_heartbeat_interval
        else:
            self._heartbeat_interval = 2
        self._connection_lock = threading.RLock()
        self._connection_host_status = {}

        if not conf.mq_hosts:
            raise ValueError("You should provide at least one RabbitMQ host")
        self._host_list = conf.mq_hosts.split(',')
        self._cur_connection_host_num = random.randint(0, len(self._host_list) - 1)

    def create_connection(self, for_listening=False):
        """Create and return connection to any available host.
        :return: created connection
        """
        with self._connection_lock:
            host_count = len(self._host_list)
            connection_attempts = host_count
            while connection_attempts > 0:
                self._cur_connection_host_num += 1
                self._cur_connection_host_num %= host_count
                connection = self.create_host_connection(self._cur_connection_host_num, for_listening)
                return connection

    def create_host_connection(self, host_index, for_listening=False):
        """Create new connection to host #host_index
        :param host_index: Integer, number of host for connection establishing
        :param for_listening: Boolean, creates connection for listening if True
        :return: New connection
        """
        with self._connection_lock:
            host = self._host_list[host_index]
            connection_params = pika.ConnectionParameters(
                host=host,
                port=self.conf.mq_port,
                credentials=pika.credentials.PlainCredentials(self.conf.mq_username, self.conf.mq_password),
                heartbeat_interval=self._heartbeat_interval if for_listening else None,
                **self._common_pika_params
            )
            try:
                if for_listening:
                    connection = None
                else:
                    connection = pika.BlockingConnection(parameters=connection_params)
                    connection.params = connection_params
                    LOG.info('connected rabbitmq-server %s:%s' % (host, self.conf.mq_port))
                return connection
            except:
                LOG.error(traceback.format_exc())


def singleton(cls):
    """ 单例装饰器

    """
    _instance_lock = threading.RLock()

    def _singleton(*args, **kwargs):
        with _instance_lock:
            if not hasattr(cls, '_instance'):
                cls._instance = cls(*args, **kwargs)
            return cls._instance

    return _singleton


# @singleton
class Transport(object):
    def __init__(self, driver):
        self._driver = driver
        self.connection = self._driver.create_connection()
        self.channel = self.connection.channel()

        self.correlation_id = None      # 消息关联ID
        self.response = None            # 响应结果
        self.consumer_tag = None        # 消费者ID

    def __del__(self):
        self.channel.close()
        self.connection.close()

    def send_rpc(self, target, message, reply_queue=None):
        """ 阻塞发送消息
        """
        self.response = None
        self.correlation_id = str(uuid.uuid4())

        # 创建目标消息队列和回调消息队列
        self.channel.queue_declare(queue=target.queue, durable=True)  # queue durable
        if reply_queue is not None:
            callback_queue = reply_queue
        else:
            callback_queue = '%s-callback' % target.queue
        self.channel.queue_declare(queue=callback_queue, durable=True)

        # 注册回调消息队列消息响应函数
        self.consumer_tag = self.channel.basic_consume(self.on_send_rpc, queue=callback_queue)

        # 发送消息
        properties = pika.BasicProperties(reply_to=callback_queue,
                                          correlation_id=self.correlation_id,
                                          content_type='application/json',
                                          delivery_mode=2)  # make message persistent
        self.channel.basic_publish(exchange='',
                                   routing_key=target.queue,
                                   properties=properties,
                                   body=json.dumps(message.data))

        # 等待返回结果
        while self.response is None:
            self.connection.process_data_events()
        return self.response

    def on_send_rpc(self, ch, method, props, body):
        """ 处理回调队列消息响应函数
        """
        if self.correlation_id == props.correlation_id:
            result = json.loads(body)
            self.response = result['body']
            # print self.response

        # 确认消息
        ch.basic_ack(delivery_tag=method.delivery_tag)
        # ch.basic_cancel(self.consumer_tag)

    def notify(self, message):
        """ 广播消息
        """
        self.channel.basic_publish(exchange='amq.fanout',
                                   routing_key='',
                                   properties=None,
                                   body=json.dumps(message.data))
        return None

    def send_request(self, target, message, reply_queue=None):
        """ 非阻塞发送消息请求
        """

        # 创建目标队列
        self.channel.queue_declare(queue=target.queue, durable=True)  # queue durable

        # 创建回调队列
        if reply_queue is not None:
            callback_queue = reply_queue
        else:
            callback_queue = '%s-callback' % target.queue
        self.channel.queue_declare(queue=callback_queue, durable=True)

        # 设置消息属性
        correlation_id = str(uuid.uuid4())
        properties = pika.BasicProperties(reply_to=callback_queue,
                                          correlation_id=correlation_id,
                                          content_type='application/json',
                                          delivery_mode=2)  # make message persistent
        # 向目标队列发送消息
        self.channel.basic_publish(exchange='',
                                   routing_key=target.queue,
                                   properties=properties,
                                   body=json.dumps(message.data))

    def receive_response(self, target, reply_queue=None):
        """ 异步接收回调队列消息

        """
        messageclient.receive_response(self, target, reply_queue)

    def send_message(self, target, message, callback_queue=None):
        """ 阻塞发送消息，返回消息响应结果

        """
        return self.send_rpc(target, message, callback_queue)

    def broadcast_message(self, message):
        """ 广播消息

        """
        return self.notify(message)


def get_transport(conf):
    """
    A factory method for Transport objects.
    :param conf: cfg.ConfigOpts, the user configuration
    :return:
    """
    _pika_engine = PikaEngine(conf)
    return Transport(_pika_engine)


class Target(object):
    """
    Identifies the destination of messages.
    A Target encapsulates all the information to identify where a message
    should be sent or what messages a server is listening for.
    """
    def __init__(self, exchange=None, topic=None, queue=None, broadcast=False):
        """
        :param exchange: str, exchange name.
        :param topic: str, exchange type, topic, direct, fanout.
        :param queue: the target queue name.
        """
        self.exchange = exchange
        self.topic = topic
        self.queue = queue
        self.broadcast = broadcast

    def __call__(self, **kwargs):
        for a in ('exchange', 'topic', 'queue'):
            kwargs.setdefault(a, getattr(self, a))
        return Target(**kwargs)

    def __repr__(self):
        attrs = []
        for a in ('exchange', 'topic', 'queue'):
            v = getattr(self, a)
            if v:
                attrs.append((a, v))
        values = ', '.join(['%s=%s' % i for i in attrs])
        return '<Target ' + values + '>'
