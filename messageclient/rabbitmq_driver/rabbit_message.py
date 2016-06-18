#!/usr/bin/python
# -*- coding: utf-8 -*-
#########################################################################
# File Name: rabbit_message.py
# Author: xuyingquan
# mail: yingquan.xu@shatacloud
# Created Time: Thu May 12 11:21:19 HKT 2016
#########################################################################


class Message(object):

    def __init__(self, transport=None, target=None, header={}, body={}):
        self.transport = transport
        self.target = target
        self.header = header                # 消息头部
        self.body = body                    # 消息数据
        self.data = dict()                  # 发送的消息
        self.data['body'] = self.body
        self.data['header'] = self.header
