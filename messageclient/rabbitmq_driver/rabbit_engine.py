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
from messageclient import LOG
import traceback


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

    def __del__(self):
        self.connection.close()


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
