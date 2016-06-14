#!/usr/bin/python
# -*- coding: utf-8 -*-
#########################################################################
# File Name: rabbit_message.py
# Author: xuyingquan
# mail: yingquan.xu@shatacloud
# Created Time: Thu May 12 11:21:19 HKT 2016
#########################################################################

import pika
import uuid
import json


class Message(object):

    def __init__(self, transport, target, header={}, body={}):
        self.transport = transport
        self.target = target
        self.channel = self.transport.channel
        self.correlation_id = None          # 消息关联id
        self.header = header                # 消息头部
        self.body = body                    # 消息数据
        self.data = dict()                  # 发送的消息
        self.data['body'] = self.body
        self.data['header'] = self.header
        self.response = None                # 消息响应结果
        self.callback_queue = '%s-callback' % self.target.queue     # 回调队列

    def send_rpc(self):
        """ 阻塞发送消息
        """
        self.response = None
        self.correlation_id = str(uuid.uuid4())

        # 创建目标消息队列和回调消息队列
        self.channel.queue_declare(queue=self.target.queue, durable=True)     # queue durable
        self.channel.queue_declare(queue=self.callback_queue, durable=True)

        # 注册回调消息队列消息响应函数
        self.channel.basic_consume(self.on_send_rpc, queue=self.callback_queue)

        # 发送消息
        properties = pika.BasicProperties(reply_to=self.callback_queue,
                                          correlation_id=self.correlation_id,
                                          content_type='application/json',
                                          delivery_mode=2)          # make message persistent
        self.channel.basic_publish(exchange='',
                                   routing_key=self.target.queue,
                                   properties=properties,
                                   body=json.dumps(self.data))

        # 等待返回结果
        while self.response is None:
            self.transport.connection.process_data_events()
        return self.response

    def on_send_rpc(self, ch, method, props, body):
        """ 处理回调队列消息响应函数
        """
        if self.correlation_id == props.correlation_id:
            self.response = json.loads(body)

        # 确认消息
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def notify(self):
        """ 广播消息
        """
        self.channel.basic_publish(exchange='amq.fanout',
                                   routing_key='',
                                   properties=None,
                                   body=json.dumps(self.data))
        return None

    def send_request(self):
        """ 非阻塞发送消息请求
        """

        # 创建目标队列
        self.channel.queue_declare(queue=self.target.queue, durable=True)  # queue durable

        # 创建回调队列
        callback_queue = '%s-callback' % self.target.queue
        self.channel.queue_declare(queue=callback_queue, durable=True)

        # 设置消息属性
        correlation_id = str(uuid.uuid4())
        properties = pika.BasicProperties(reply_to=callback_queue,
                                          correlation_id=correlation_id,
                                          content_type='application/json',
                                          delivery_mode=2)  # make message persistent
        # 向目标队列发送消息
        self.channel.basic_publish(exchange='',
                                   routing_key=self.target.queue,
                                   properties=properties,
                                   body=json.dumps(self.data))


