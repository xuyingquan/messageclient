#!/usr/bin/python
# -*- coding: utf-8 -*-
#########################################################################
# File Name: __init__.py
# Author: xuyingquan
# mail: yingquan.xu@shatacloud
# Created Time: Sat May  7 10:33:08 CST 2016
#########################################################################

import util
import pika
import json
import traceback
import threading
LOG = util.init_logger('messageclient', '/var/log/messageclient.log')

from messageclient.rabbitmq_driver.rabbit_engine import PikaEngine, Target, Transport
from messageclient.rabbitmq_driver.rabbit_engine import get_transport
from messageclient.rabbitmq_driver.rabbit_message import Message
from messageclient.rabbitmq_driver import Consumer, Publisher, RpcPublisher, RpcConsumer
from messageclient.rabbitmq_driver import on_message_v1


message_handler = dict()            # 消息处理函数（用户定义）字典

__all__ = [
    "Target",
    "Transport",
    "Message",
    "LOG",
    "send_message",
    "send_rpc_response",
    "start_consume_message",
    "get_transport",
    "on_message",
    "send_request",
    "receive_response",
]


@util.timeout_decorator
def send_message(message, mode='rpc'):
    """ 发送消息
    :param message: 消息对象
    :param mode: 消息发送方式，rpc，单播或者广播
    :return: 返回消息响应结果

    """
    if mode == 'rpc':
        return message.transport.send_rpc(message.target, message)
    elif mode == 'notify':
        return message.transport.notify(message)
    elif mode == 'async':
        return message.transport.send_request(message)
    else:
        return None


def on_message(type=None):
    """ 装饰器，装饰消息响应函数，将装饰的响应函数加入到routes字典，以type为关键字
    :param type: 消息类型
    :return: 返回封装后的消息响应函数 __decorator(message)

    """
    def _decorator(handle_message):
        def __decorator(message):
            result = handle_message(message)
            return result
        if type is not None:
            if type in message_handler:
                message_handler['%s-callback' % type] = __decorator
            else:
                message_handler[type] = __decorator
        return __decorator

    return _decorator


def send_rpc_response(ch, method, props, result):
    """ 给发送端返回消息响应结果，并对接受到的消息进行确认

    """
    message_properties = pika.BasicProperties(correlation_id=props.correlation_id)
    callback_queue = props.reply_to

    # 返回处理结果
    ch.basic_publish(exchange='',
                     routing_key=callback_queue,
                     properties=message_properties,
                     body=json.dumps(result))
    # 确认消息
    ch.basic_ack(delivery_tag=method.delivery_tag)


def on_message_received(ch, method, props, msg):
    """ 接受消息响应函数， 此函数由pika调用； 根据消息的类型调用具体的消息响应函数

    """
    try:
        msg = json.loads(msg)
        msg_body = msg['body']                      # 获取消息体数据
        msg_type = msg['header']['type']            # 获取消息的类型

        _do_task = message_handler[msg_type]        # 获取特定消息类型的处理函数
        result = _do_task(msg_body)                 # 处理消息
        if result is None:
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return
        result_msg = dict()                         # 封装返回结果，添加头部信息
        result_msg['body'] = result
        result_msg['header'] = msg['header']

        send_rpc_response(ch, method, props, result_msg)    # 返回发送端处理结果
    except:
        LOG.error(traceback.format_exc())


def consume_message(transport, target, callback):
    """ 准备消费消息

    """
    # daemonize()

    # 创建消费队列
    transport.channel.queue_declare(queue=target.queue, durable=True)
    if target.broadcast:
        transport.channel.queue_bind(exchange='amq.fanout', queue=target.queue)

    # 设置队列预取消息
    transport.channel.basic_qos(prefetch_count=1)

    # 注册消息响应函数
    transport.channel.basic_consume(callback, queue=target.queue)
    LOG.info('waiting rpc request...')

    # 开始消费消息
    try:
        transport.channel.start_consuming()
    except:
        LOG.error(traceback.format_exc())
        transport.channel.stop_consuming()
    transport.connection.close()


def start_consume_message(transport, target):
    """ 监听特定队列，处理消息
    :param transport: 连接传输对象.
    :param target: 目标对象.
    :param callback: 处理消息回调函数

    """
    threading.Thread(target=consume_message, args=(transport, target, on_message_received)).start()


def on_response_received(ch, method, props, msg):
    """ 接受消息响应函数， 此函数由pika调用； 根据消息的类型调用具体的消息响应函数

    """
    try:
        msg = json.loads(msg)
        msg_body = msg['body']                      # 获取消息体数据
        msg_type = msg['header']['type']            # 获取消息的类型
        msg_type_callback = '%s-callback' % msg_type
        if msg_type_callback in message_handler:
            _do_task = message_handler[msg_type_callback]        # 获取特定消息类型的处理函数
        else:
            _do_task = message_handler[msg_type]
        _do_task(msg_body)                                       # 处理响应结果
        ch.basic_ack(delivery_tag=method.delivery_tag)           # 确认收到的消息
    except:
        LOG.error(traceback.format_exc())


def consume_callback_message(transport, target):
    """ 消费回调队列消息

    """
    # 创建回调队列
    callback_queue = '%s-callback' % target.queue
    transport.channel.queue_declare(queue=callback_queue, durable=True)
    if target.broadcast:
        transport.channel.queue_bind(exchange='amq.fanout', queue=target.queue)

    # 注册消息处理函数
    transport.channel.basic_qos(prefetch_count=1)
    transport.channel.basic_consume(on_response_received, queue=callback_queue)
    LOG.info('waiting callback response...')

    # 接收处理消息
    try:
        transport.channel.start_consuming()
    except:
        LOG.error(traceback.format_exc())
        transport.channel.stop_consuming()
    transport.connection.close()


def send_request(message):
    """ 发送异步消息
    :param message: 消息对象

    """
    message.transport.send_request(message.target, message)


def receive_response(transport, target):
    """ 接收异步消息返回结果
    """
    threading.Thread(target=consume_callback_message, args=(transport, target)).start()

