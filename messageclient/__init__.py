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
target = messageclient.Target(**kwargs)
message = messageclient.Message(transport, target, msg_body)
messageclient.send_message(message, mode=unicast)
"""
import log
import os
import sys
import pika
import json
import traceback
LOG = log.init_logger('messageclient', '/var/log/messageclient.log')

from messageclient.rabbitmq_driver.rabbit_engine import PikaEngine, Target, Transport
from messageclient.rabbitmq_driver.rabbit_engine import get_transport
from messageclient.rabbitmq_driver.rabbit_message import Message


__all__ = [
    "Target",
    "Transport",
    "Message",
    "LOG",
    "send_message",
    "send_rpc_response",
    "start_consume_message",
    "get_transport",
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


def send_rpc_response(ch, method, props, result):
    message_properties = pika.BasicProperties(correlation_id=props.correlation_id)
    callback_queue = props.reply_to
    ch.basic_publish(exchange='',
                     routing_key=callback_queue,
                     properties=message_properties,
                     body=json.dumps(result))
    ch.basic_ack(delivery_tag=method.delivery_tag)


def start_consume_message(transport, target, callback):
    daemonize()
    transport.channel.queue_declare(queue=target.appname)
    transport.channel.basic_qos(prefetch_count=1)
    transport.channel.basic_consume(callback, queue=target.appname)
    LOG.info('waiting rpc request...')
    try:
        transport.channel.start_consuming()
    except:
        LOG.error(traceback.format_exc())



