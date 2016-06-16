#!/usr/bin/python
# -*- coding: utf-8 -*-
#########################################################################
# File Name: __init__.py
# Author: xuyingquan
# mail: yingquan.xu@shatacloud
# Created Time: Wed Jun  1 17:32:26 CST 2016
#########################################################################

import pika
from messageclient import LOG
import threading
import json
import time
import uuid
import sys
from messageclient import util

LOGGER = LOG
CALLBACK_MANAGER = dict()           # 消息处理函数集合


def on_message_v1(type=None):
    """ 装饰器，装饰用户定义的消息处理函数，将其加入到CALLBACK_MANAGER中，供on_message调用

    """
    def _decorator(fn):
        if not util.is_callable(fn):
            LOG.error('function %s is not callable' % fn)
            sys.exit(-1)

        def __decorator(self, message):
            result = fn(self, message)
            return result

        # 将被装饰的用户定义的函数注册到
        if type is not None and not type in CALLBACK_MANAGER:
            CALLBACK_MANAGER[type] = __decorator

        return __decorator
    return _decorator


class RabbitMessage(object):
    """ 消息类型

    """
    count = 0       # 消息计数

    def __init__(self, header={}, body={}):
        RabbitMessage.count += 1
        self.id = RabbitMessage.count       # 消息id
        self.msg = dict()                   # 封装消息数据
        self.msg['header'] = header
        self.msg['body'] = body


class Consumer(threading.Thread):
    """ 消息消费者基类

    """
    def __init__(self, conf, queue, exchange=None, exchange_type='topic', binding_key=None):
        """ 构造函数
        :param conf: ConfigOpts, 配置文件对象
        :param queue: str, 连接队列名称
        :param exchange: str, 交换机名称
        :param exchange_type: str, 交换机类型
        :param binding_key: str, 交换机和队列绑定的 binding_key

        """
        super(Consumer, self).__init__()
        self.conf = conf

        # 如果没有指定交换机，默认创建和队列名称相同的交换机
        self.exchange = queue if exchange is None else exchange
        self.exchange_type = exchange_type
        self.queue = queue

        # 指定消息的routing_key和交换机队列的binding_key相同
        if binding_key:
            self.routing_key = binding_key
        elif exchange and queue:
            # 如果没有指定binding_key,将routing_key设置成exchange-queue
            self.routing_key = '%s-%s' % (exchange, queue)
        else:
            # 如果没有指定exchange和binding_key，将routing_key设置成与队列同名
            self.routing_key = queue
        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None       # 消费者标记
        self.start()                    # 启动消费者，执行self.run方法

    def __del__(self):
        """ 析构函数，释放通道和连接

        """
        self.close_channel()
        self.close_connection()

    def connect(self):
        """ 连接RabbitMQ, 返回连接句柄. 当连接建立后，on_connection_open方法将会被调用

        """
        LOGGER.info('Connecting to %s' % self.conf.mq_hosts)
        connection_params = pika.ConnectionParameters(
            host=self.conf.mq_hosts,
            port=self.conf.mq_port,
            credentials=pika.credentials.PlainCredentials(self.conf.mq_username, self.conf.mq_password)
        )
        return pika.SelectConnection(parameters=connection_params,
                                     on_open_callback=self.on_connection_open,
                                     on_open_error_callback=None,
                                     on_close_callback=None,
                                     stop_ioloop_on_close=False)

    def on_connection_open(self, connection):
        """ 连接建立成功后，该方法被调用； 注册连接关闭响应函数以及建立通道

        """
        LOGGER.info('Connection opened')
        self.add_on_connection_close_callback()
        self.open_channel()

    def add_on_connection_close_callback(self):
        """ 注册连接关闭响应函数

        """
        LOGGER.info('Adding connection close callback')
        self._connection.add_on_close_callback(self.on_connection_closed)

    def on_connection_closed(self, connection, reply_code, reply_text):
        """ 连接关闭响应函数

        """
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            LOGGER.warning('Connection closed, reopening in 5 seconds: (%s) %s' % (reply_code, reply_text))
            self._connection.add_timeout(5, self.reconnect)

    def reconnect(self):
        """ 当连接关闭时，重连RabbitMQ

        """

        self._connection.ioloop.stop()          # 停止之前的ioloop实例

        if not self._closing:
            self._connection = self.connect()   # 创建新的连接
            self._connection.ioloop.start()     # 在新的连接上启动ioloop

    def open_channel(self):
        """ 建立连接通道，给RabbitMQ发送Channel.Open命令，当接收到Channel.Open.OK时表示通道已建立

        """
        LOGGER.info('Creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        """ 当收到Channel.Open.OK命令时，会调用该函数

        """
        LOGGER.info('Channel opened')
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self.exchange)

    def add_on_channel_close_callback(self):
        """ 注册连接通道关闭响应函数

        """
        LOGGER.info('Adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reply_code, reply_text):
        """ 连接通道关闭响应函数, 在这里我们仅仅做了关闭连接

        """
        LOGGER.warning('Channel %i was closed: (%s) %s' % (channel, reply_code, reply_text))
        self._connection.close()

    def setup_exchange(self, exchange_name):
        """ 创建交换机，向RabbitMQ发送Exchange.Declare命令

        """
        LOGGER.info('Declaring exchange %s' % exchange_name)
        self._channel.exchange_declare(self.on_exchange_declareok,
                                       exchange_name,
                                       self.exchange_type,
                                       durable=True)

    def on_exchange_declareok(self, method_frame):
        """ 交换机创建成功响应函数, 会接收到Exchange.DeclareOk命令

        """
        LOGGER.info('Exchange declared')
        self.setup_queue(self.queue)

    def setup_queue(self, queue_name):
        """ 创建队列，向RabbitMQ发送Queue.Declare命令

        """
        LOGGER.info('Declaring queue %s' % queue_name)
        self._channel.queue_declare(self.on_queue_declareok, queue_name, durable=True)

    def on_queue_declareok(self, method_frame):
        """ 队列创建完成响应函数，接收RabbitMQ发送过来的Queue.DeclareOk命令

        """
        LOGGER.info('Binding %s to %s with %s' % (self.exchange, self.queue, self.routing_key))
        self._channel.queue_bind(self.on_bindok, self.queue, self.exchange, self.routing_key)

    def on_bindok(self, method_frame):
        """ 交换机和队列绑定成功响应函数

        """
        LOGGER.info('Queue bound')
        self.start_consuming()

    def start_consuming(self):
        """ 准备消费消息

        """
        LOGGER.info('Issuing consumer related RPC commands')
        self.add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(self.on_message, self.queue)

    def add_on_cancel_callback(self):
        """ 注册注销消费者响应函数

        """
        LOGGER.info('Adding consumer cancellation callback')
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        """ 注销消费者响应函数

        """
        LOGGER.info('Consumer was cancelled remotely, shutting down: %r' % method_frame)
        if self._channel:
            self._channel.close()

    def on_message(self, channel, method, props, body):
        """ 消息处理函数， 每当接收到一个完整消息后会调用该函数

        """
        message = json.loads(body)
        self.handle_message(message)
        self.acknowledge_message(delivery_tag=method.delivery_tag)

    def handle_message(self, message):
        print 'receive message: %s' % message
        return message

    def acknowledge_message(self, delivery_tag):
        """ 对收到的消息进行确认

        """
        LOGGER.info('Acknowledging message %s' % delivery_tag)
        self._channel.basic_ack(delivery_tag)

    def stop_consuming(self):
        """ 注销当前消费者，向RabbitMQ发送Basic.Cancel命令

        """
        if self._channel:
            LOGGER.info('Sending a Basic.Cancel RPC command to RabbitMQ')
            self._channel.basic_cancel(self.on_cancelok, self._consumer_tag)

    def on_cancelok(self, method_frame):
        """ 注销消费者成功响应函数，收到Basic.CancelOk命令

        """
        LOGGER.info('RabbitMQ acknowledged the cancellation of the consumer')
        self.close_channel()

    def close_channel(self):
        """ 主动关闭连接通道，发送Channel.Close命令给RabbitMQ

        """
        LOGGER.info('Closing the channel')
        self._channel.close()

    def run(self):
        """ 建立到RabbitMQ的连接，启动IOLoop阻塞等待SelectConnection处理

        """
        self._connection = self.connect()
        self._connection.ioloop.start()

    def stop(self):
        """ 主动注销消费者并关闭连接，当RabbitMQ确认注销消费者后，on_cancelok函数
        将会被调用，在这个函数里关闭连接通道和连接

        """
        LOGGER.info('Stopping')
        self._closing = True
        self.stop_consuming()
        self._connection.ioloop.start()
        LOGGER.info('Stopped')

    def close_connection(self):
        """ 主动关闭连接

        """
        LOGGER.info('Closing connection')
        self._connection.close()


class Publisher(threading.Thread):
    """ 消息生产者基类

    """
    def __init__(self, conf, queue, exchange=None, exchange_type='topic', binding_key=None):
        """ 构造函数
        :param conf: ConfigOpts, 配置文件对象
        :param queue: str, 连接队列名称
        :param exchange: str, 交换机名称
        :param exchange_type: str, 交换机类型
        :param binding_key: str, 交换机和队列绑定的 binding_key

        """
        super(Publisher, self).__init__()
        self.conf = conf

        # 如果没有指定交换机，默认创建和队列名称相同的交换机
        self.exchange = queue if exchange is None else exchange
        self.exchange_type = exchange_type
        self.queue = queue

        # 指定消息的routing_key和交换机队列的binding_key相同
        if binding_key:
            self.routing_key = binding_key
        elif exchange and queue:
            # 如果没有指定binding_key,将routing_key设置成exchange-queue
            self.routing_key = '%s-%s' % (exchange, queue)
        else:
            # 如果没有指定exchange和binding_key，将routing_key设置成与队列同名
            self.routing_key = queue
        self._connection = None
        self._channel = None
        self._deliveries = []
        self._acked = 0
        self._nacked = 0
        self._message_number = 0
        self._stopping = False
        self._closing = False
        self.start()                    # 启动生产者，执行self.run方法

    def __del__(self):
        """ 析构函数，释放通道和连接

        """
        self.close_channel()
        self.close_connection()

    def connect(self):
        """ 连接RabbitMQ, 返回连接句柄. 当连接建立后，on_connection_open方法将会被调用

        """
        LOG.info('Connecting to %s' % self.conf.mq_hosts)
        connection_params = pika.ConnectionParameters(
            host=self.conf.mq_hosts,
            port=self.conf.mq_port,
            credentials=pika.credentials.PlainCredentials(self.conf.mq_username, self.conf.mq_password)
        )
        return pika.SelectConnection(parameters=connection_params,
                                     on_open_callback=self.on_connection_open,
                                     on_open_error_callback=None,
                                     on_close_callback=None,
                                     stop_ioloop_on_close=False)

    def on_connection_open(self, connection):
        """ 连接建立成功后，该方法被调用； 注册连接关闭响应函数以及建立通道

        """
        LOG.info('Connection opened')
        self.add_on_connection_close_callback()
        self.open_channel()

    def add_on_connection_close_callback(self):
        """ 注册连接关闭响应函数

        """
        LOG.info('Adding connection close callback')
        self._connection.add_on_close_callback(self.on_connection_closed)

    def on_connection_closed(self, connection, reply_code, reply_text):
        """ 连接关闭响应函数

        """
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            LOG.warning('Connection closed, reopening in 5 seconds: (%s) %s' % (reply_code, reply_text))
            self._connection.add_timeout(5, self.reconnect)

    def reconnect(self):
        """ 当连接关闭时，重连RabbitMQ

        """
        self._deliveries = []
        self._acked = 0
        self._nacked = 0
        self._message_number = 0
        self._connection.ioloop.stop()
        self._connection = self.connect()
        self._connection.ioloop.start()

    def open_channel(self):
        """ 建立连接通道，给RabbitMQ发送Channel.Open命令，当接收到Channel.Open.OK时表示通道已建立

        """
        LOG.info('Creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        """ 当收到Channel.Open.OK命令时，会调用该函数

        """
        LOG.info('Channel opened')
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self.exchange)

    def add_on_channel_close_callback(self):
        """ 注册连接通道关闭响应函数

        """
        LOG.info('Adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reply_code, reply_text):
        """ 连接通道关闭响应函数, 在这里我们仅仅做了关闭连接

        """
        LOG.warning('Channel was closed: (%s) %s' % (reply_code, reply_text))
        if not self._closing:
            self._connection.close()

    def setup_exchange(self, exchange_name):
        """ 创建交换机，向RabbitMQ发送Exchange.Declare命令

        """
        LOG.info('Declaring exchange %s' % exchange_name)
        self._channel.exchange_declare(self.on_exchange_declareok, exchange_name, self.exchange_type, durable=True)

    def on_exchange_declareok(self, method_frame):
        """ 交换机创建成功响应函数, 会接收到Exchange.DeclareOk命令

        """
        LOG.info('Exchange declared')
        self.setup_queue(self.queue)

    def setup_queue(self, queue_name):
        """ 创建队列，向RabbitMQ发送Queue.Declare命令

        """
        LOG.info('Declaring queue %s' % queue_name)
        self._channel.queue_declare(self.on_queue_declareok, queue_name, durable=True)

    def on_queue_declareok(self, method_frame):
        """ 队列创建完成响应函数，接收RabbitMQ发送过来的Queue.DeclareOk命令

        """
        LOG.info('Binding %s to %s with %s' % (self.exchange, self.queue, self.routing_key))
        self._channel.queue_bind(self.on_bindok, self.queue, self.exchange, self.routing_key)

    def on_bindok(self, method_frame):
        """ 交换机和队列绑定成功响应函数

        """
        LOG.info('Queue bound')
        self.start_publishing()

    def start_publishing(self):
        LOG.info('Issuing consumer related RPC commands')
        self.enable_delivery_confirmations()
        # self.schedule_next_message()

    def enable_delivery_confirmations(self):
        LOG.info('Issuing Confirm.Select RPC command')
        self._channel.confirm_delivery(self.on_delivery_confirmation)

    def on_delivery_confirmation(self, method_frame):
        """ 当收到Basic.ACK命令时， 该函数被调用； RabbitMQ响应Basic.Publish命令

        """
        confirmation_type = method_frame.method.NAME.split('.')[1].lower()
        LOG.info('Received %s for delivery tag: %i' % (confirmation_type, method_frame.method.delivery_tag))
        if confirmation_type == 'ack':
            self._acked += 1
        elif confirmation_type == 'nack':
            self._nacked += 1
        self._deliveries.remove(method_frame.method.delivery_tag)
        LOG.info('Published %i messages, %i have yet to be confirmed, %i were acked and %i were nacked'
                 % (self._message_number, len(self._deliveries), self._acked, self._nacked))

    def publish_message(self, message, routing_key=None):
        routing_key = self.routing_key if routing_key is None else routing_key
        if self._stopping:
            return
        while not self._channel:
            time.sleep(1)
        properties = pika.BasicProperties(app_id=None, content_type='application/json', headers=None)
        self._channel.basic_publish(self.exchange,
                                    routing_key,
                                    json.dumps(message, ensure_ascii=False),
                                    properties)
        self._message_number += 1
        self._deliveries.append(self._message_number)
        LOG.info('Published message # %i' % self._message_number)

    def close_channel(self):
        """ 主动关闭连接通道，发送Channel.Close命令给RabbitMQ

        """
        LOG.info('Closing the channel')
        if self._channel:
            self._channel.close()

    def run(self):
        """ 建立到RabbitMQ的连接，启动IOLoop阻塞等待SelectConnection处理

        """
        self._connection = self.connect()
        self._connection.ioloop.start()

    def stop(self):
        """ 主动注销消费者并关闭连接，当RabbitMQ确认注销消费者后，on_cancelok函数
            将会被调用，在这个函数里关闭连接通道和连接

        """
        LOG.info('Stopping')
        self._stopping = True
        self.close_channel()
        self.close_connection()
        self._connection.ioloop.start()
        LOG.info('Stopped')

    def close_connection(self):
        """ 主动关闭连接

        """
        LOG.info('Closing connection')
        self._closing = True
        self._connection.close()


class RpcConsumer(Consumer):
    def __init__(self, conf, queue):
        super(RpcConsumer, self).__init__(conf, queue)

    def __del__(self):
        self._channel.queue_delete(queue=self.queue)
        self._channel.exchange_delete(self.exchange)

    def on_message(self, channel, method, props, body):
        message = json.loads(body)
        result = self.handle_message(message)
        self.acknowledge_message(delivery_tag=method.delivery_tag)
        channel.basic_publish(exchange=self.exchange,
                              routing_key=props.reply_to,
                              properties=pika.BasicProperties(correlation_id=props.correlation_id),
                              body=json.dumps(result))


class RpcPublisher(Publisher):
    def __init__(self, conf, queue, callback_queue):
        super(RpcPublisher, self).__init__(conf, queue)
        self.callback_queue = callback_queue
        self._consumer_tag = None
        self.response = None
        self.correlation_id = None
        self.setup_queue_rpc(callback_queue)

    def __del__(self):
        self._channel.queue_delete(queue=self.callback_queue)

    def setup_queue_rpc(self, queue_name):
        """ 创建队列，向RabbitMQ发送Queue.Declare命令

        """
        LOG.info('Declaring queue %s' % queue_name)
        while not self._channel:
            time.sleep(1)
        self._channel.queue_declare(self.on_queue_declareok_rpc, queue_name, durable=True)

    def on_queue_declareok_rpc(self, method_frame):
        """ 队列创建完成响应函数，接收RabbitMQ发送过来的Queue.DeclareOk命令

        """
        LOG.info('Binding %s to %s with %s' % (self.exchange, self.callback_queue, self.callback_queue))
        self._channel.queue_bind(self.on_bindok_rpc, self.callback_queue, self.exchange, self.callback_queue)

    def on_bindok_rpc(self, unused_frame):
        """ 交换机和队列绑定成功响应函数

        """
        LOG.info('Queue bound')
        self.start_consuming_rpc()

    def start_consuming_rpc(self):
        """ 准备消费消息

        """
        LOG.info('Issuing consumer related RPC commands')
        self.add_on_cancel_callback_rpc()
        self._consumer_tag = self._channel.basic_consume(self.on_message, self.callback_queue)

    def add_on_cancel_callback_rpc(self):
        """ 注册注销消费者响应函数

        """
        LOG.info('Adding consumer cancellation callback')
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled_rpc)

    def on_consumer_cancelled_rpc(self, method_frame):
        """ 注销消费者响应函数

        """
        LOG.info('Consumer was cancelled remotely, shutting down: %r' % method_frame)
        if self._channel:
            self._channel.close()

    def on_message(self, channel, method, props, body):
        """ 消息处理函数， 每当接收到一个完整消息后会调用该函数

        """
        if props.correlation_id == self.correlation_id:
            self.response = json.loads(body)
        channel.basic_ack(delivery_tag=method.delivery_tag)

    def publish_message(self, message, routing_key=None):
        routing_key = self.routing_key if routing_key is None else routing_key
        if self._stopping:
            return
        while not self._channel:
            time.sleep(0.2)
        self.response = None
        self.correlation_id = str(uuid.uuid4())
        properties = pika.BasicProperties(app_id=None,
                                          reply_to=self.callback_queue,
                                          correlation_id=self.correlation_id,
                                          content_type='application/json',
                                          headers=None)
        self._channel.basic_publish(self.exchange,
                                    routing_key,
                                    json.dumps(message, ensure_ascii=False),
                                    properties)
        self._message_number += 1
        self._deliveries.append(self._message_number)
        LOG.info('Published message # %i' % self._message_number)

    def send_message(self, message):
        self.publish_message(message)
        while self.response is None:
            time.sleep(0.2)
        return self.response
