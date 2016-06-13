#!/usr/bin/python
# -*- coding: utf-8 -*-
#########################################################################
# File Name: __init__.py
# Author: xuyingquan
# mail: yingquan.xu@shatacloud
# Created Time: Sat May  7 10:33:08 CST 2016
#########################################################################

"""
transport = messageclient.get_transport(conf)
target = messageclient.Target(appname='IaasService')
message = messageclient.Message(transport, target, msg_body)
messageclient.send_message(message, mode='rpc')
"""

import util
import os
import sys
import pika
import json
import traceback
import threading
LOG = util.init_logger('messageclient', '/var/log/messageclient.log')

from messageclient.rabbitmq_driver.rabbit_engine import PikaEngine, Target, Transport
from messageclient.rabbitmq_driver.rabbit_engine import get_transport
from messageclient.rabbitmq_driver.rabbit_message import Message
from messageclient.rabbitmq_driver import Consumer, Publisher, RpcPublisher, RpcConsumer
from messageclient import *


event = threading.Event()           # use for protect global variable g_result
g_result = None
routes = dict()

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
    "on_response",
    "routes",
]


def daemonize(home_dir='.', umask=022, stdin=os.devnull, stdout=os.devnull, stderr=os.devnull):
    try:
        pid = os.fork()
        if pid > 0:
            # Exit first parent
            sys.exit(0)
    except OSError, e:
        sys.stderr.write("fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))
        sys.exit(1)

    # Decouple from parent environment
    os.chdir(home_dir)
    os.setsid()
    os.umask(umask)

    # Do second fork
    try:
        pid = os.fork()
        if pid > 0:
            # Exit from second parent
            sys.exit(0)
    except OSError, e:
        sys.stderr.write("fork #2 failed: %d (%s)\n" % (e.errno, e.strerror))
        sys.exit(1)

    if sys.platform != 'darwin':  # This block breaks on OS X
        # Redirect standard file descriptors
        sys.stdout.flush()
        sys.stderr.flush()
        si = file(stdin, 'r')
        so = file(stdout, 'a+')
        if stderr:
            se = file(stderr, 'a+', 0)
        else:
            se = so
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())


def send_message(message, mode='rpc'):
    if mode == 'rpc':
        return message.send_rpc()
    elif mode == 'notify':
        return message.notify()
    else:
        return None

"""
def on_message(handle_message):
    def _decorator(ch, method, props, body):
        try:
            info = json.loads(body)
            result = handle_message(info)
            send_rpc_response(ch, method, props, result)
        except:
            LOG.error(traceback.format_exc())
    return _decorator
"""


def on_message(type=None):
    def _decorator(handle_message):
        def __decorator(message):
            result = handle_message(message)
            if type is not None:
                routes[type] = __decorator
            return result
        return __decorator
    return _decorator


def on_message_broadcast(handle_message):
    def _decorator(ch, method, props, body):
        try:
            info = json.loads(body)
            handle_message(info)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except:
            LOG.error(traceback.format_exc())
    return _decorator


def send_rpc_response(ch, method, props, result):
    message_properties = pika.BasicProperties(correlation_id=props.correlation_id)
    callback_queue = props.reply_to
    ch.basic_publish(exchange='',
                     routing_key=callback_queue,
                     properties=message_properties,
                     body=json.dumps(result))
    ch.basic_ack(delivery_tag=method.delivery_tag)


def consume_message(transport, target, callback):
    # daemonize()
    transport.channel.queue_declare(queue=target.queue, durable=True)
    if target.broadcast:
        transport.channel.queue_bind(exchange='amq.fanout', queue=target.queue)
    transport.channel.basic_qos(prefetch_count=1)
    transport.channel.basic_consume(callback, queue=target.queue)
    LOG.info('waiting rpc request...')
    try:
        transport.channel.start_consuming()
    except:
        LOG.error(traceback.format_exc())
        transport.channel.stop_consuming()
    transport.connection.close()


def consumer(ch, method, props, body):
    try:
        info = json.loads(body)
        LOG.info('routes: %s' % routes)
        type = info['header']['type']
        handle_message = routes[type]

        result = handle_message(info)
        send_rpc_response(ch, method, props, result)
    except:
        LOG.error(traceback.format_exc())


def start_consume_message(transport, target):
    """
    listening quque to handle message.
    :param transport: Transport object for connection.
    :param target: Target object.
    :param callback: handle message.
    :return: None
    """
    threading.Thread(target=consume_message, args=(transport, target, consumer)).start()

"""
def send_message_async(message):
    global g_result
    event.clear()
    g_result = send_message(message, mode='rpc')
    # print 'g_result: %s ' % g_result
    event.set()     # notify receiver
"""


def send_request(message):
    """
    send asynchronous message
    :param message: Message object
    :return: None
    """
    message.send_request()


def on_response(handle_request):
    def _decorator(ch, method, props, body):
        try:
            info = json.loads(body)
            handle_request(info)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except:
            LOG.error(traceback.format_exc())

    return _decorator


def receive_response_async(transport, target, callback):
    """
    receive asynchronous message
    :param callback: use for handling message received asynchronously
    :return:
    """
    callback_queue = '%s-callback' % target.queue
    transport.channel.queue_declare(queue=callback_queue, durable=True)
    if target.broadcast:
        transport.channel.queue_bind(exchange='amq.fanout', queue=target.queue)
    transport.channel.basic_qos(prefetch_count=1)
    transport.channel.basic_consume(callback, queue=callback_queue)
    LOG.info('waiting callback response...')
    try:
        transport.channel.start_consuming()
    except:
        LOG.error(traceback.format_exc())
        transport.channel.stop_consuming()
    transport.connection.close()


def receive_response(transport, target, callback):
    threading.Thread(target=receive_response_async, args=(transport, target, callback)).start()

