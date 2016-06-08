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

LOGGER = LOG


class Consumer(threading.Thread):
    def __init__(self, conf, queue, exchange=None, exchange_type='topic', binding_key=None):
        super(Consumer, self).__init__()
        self.conf = conf
        self.exchange = queue if exchange is None else exchange
        self.exchange_type = exchange_type
        self.queue = queue
        if binding_key:
            self.routing_key = binding_key
        elif exchange and queue:
            self.routing_key = '%s-%s' % (exchange, queue)
        else:
            self.routing_key = queue
        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self.start()

    def __del__(self):
        self.close_channel()
        self.close_connection()

    def connect(self):
        """This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika.

        :rtype: pika.SelectConnection
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

    def on_connection_open(self, unused_connection):
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it, but in this case, we'll just mark it unused.

        :type unused_connection: pika.SelectConnection
        """
        LOGGER.info('Connection opened')
        self.add_on_connection_close_callback()
        self.open_channel()

    def add_on_connection_close_callback(self):
        """This method adds an on close callback that will be invoked by pika
        when RabbitMQ closes the connection to the publisher unexpectedly.
        """
        LOGGER.info('Adding connection close callback')
        self._connection.add_on_close_callback(self.on_connection_closed)

    def on_connection_closed(self, connection, reply_code, reply_text):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.

        :param pika.connection.Connection connection: The closed connection obj
        :param int reply_code: The server provided reply_code if given
        :param str reply_text: The server provided reply_text if given
        """
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            LOGGER.warning('Connection closed, reopening in 5 seconds: (%s) %s' % (reply_code, reply_text))
            self._connection.add_timeout(5, self.reconnect)

    def reconnect(self):
        """Will be invoked by the IOLoop timer if the connection is
        closed. See the on_connection_closed method.

        """
        # This is the old connection IOLoop instance, stop its ioloop
        self._connection.ioloop.stop()

        if not self._closing:
            # Create a new connection
            self._connection = self.connect()

            # There is now a new connection, needs a new ioloop to run
            self._connection.ioloop.start()

    def open_channel(self):
        """Open a new channel with RabbitMQ by issuing the Channel.Open RPC
        command. When RabbitMQ responds that the channel is open, the
        on_channel_open callback will be invoked by pika.

        """
        LOGGER.info('Creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.

        Since the channel is now open, we'll declare the exchange to use.

        :param pika.channel.Channel channel: The channel object

        """
        LOGGER.info('Channel opened')
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self.exchange)

    def add_on_channel_close_callback(self):
        """This method tells pika to call the on_channel_closed method if
        RabbitMQ unexpectedly closes the channel.

        """
        LOGGER.info('Adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reply_code, reply_text):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.

        :param pika.channel.Channel: The closed channel
        :param int reply_code: The numeric reason the channel was closed
        :param str reply_text: The text reason the channel was closed

        """
        LOGGER.warning('Channel %i was closed: (%s) %s' % (channel, reply_code, reply_text))
        self._connection.close()

    def setup_exchange(self, exchange_name):
        """Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.

        :param str|unicode exchange_name: The name of the exchange to declare

        """
        LOGGER.info('Declaring exchange %s' % exchange_name)
        self._channel.exchange_declare(self.on_exchange_declareok,
                                       exchange_name,
                                       self.exchange_type,
                                       durable=True)

    def on_exchange_declareok(self, unused_frame):
        """Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
        command.

        :param pika.Frame.Method unused_frame: Exchange.DeclareOk response frame

        """
        LOGGER.info('Exchange declared')
        self.setup_queue(self.queue)

    def setup_queue(self, queue_name):
        """Setup the queue on RabbitMQ by invoking the Queue.Declare RPC
        command. When it is complete, the on_queue_declareok method will
        be invoked by pika.

        :param str|unicode queue_name: The name of the queue to declare.

        """
        LOGGER.info('Declaring queue %s' % queue_name)
        self._channel.queue_declare(self.on_queue_declareok, queue_name, durable=True)

    def on_queue_declareok(self, method_frame):
        """Method invoked by pika when the Queue.Declare RPC call made in
        setup_queue has completed. In this method we will bind the queue
        and exchange together with the routing key by issuing the Queue.Bind
        RPC command. When this command is complete, the on_bindok method will
        be invoked by pika.

        :param pika.frame.Method method_frame: The Queue.DeclareOk frame

        """
        LOGGER.info('Binding %s to %s with %s' % (self.exchange, self.queue, self.routing_key))
        self._channel.queue_bind(self.on_bindok, self.queue, self.exchange, self.routing_key)

    def on_bindok(self, unused_frame):
        """Invoked by pika when the Queue.Bind method has completed. At this
        point we will start consuming messages by calling start_consuming
        which will invoke the needed RPC commands to start the process.

        :param pika.frame.Method unused_frame: The Queue.BindOk response frame

        """
        LOGGER.info('Queue bound')
        self.start_consuming()

    def start_consuming(self):
        """This method sets up the consumer by first calling
        add_on_cancel_callback so that the object is notified if RabbitMQ
        cancels the consumer. It then issues the Basic.Consume RPC command
        which returns the consumer tag that is used to uniquely identify the
        consumer with RabbitMQ. We keep the value to use it when we want to
        cancel consuming. The on_message method is passed in as a callback pika
        will invoke when a message is fully received.

        """
        LOGGER.info('Issuing consumer related RPC commands')
        self.add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(self.on_message, self.queue)

    def add_on_cancel_callback(self):
        """Add a callback that will be invoked if RabbitMQ cancels the consumer
        for some reason. If RabbitMQ does cancel the consumer,
        on_consumer_cancelled will be invoked by pika.

        """
        LOGGER.info('Adding consumer cancellation callback')
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        """Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer
        receiving messages.

        :param pika.frame.Method method_frame: The Basic.Cancel frame

        """
        LOGGER.info('Consumer was cancelled remotely, shutting down: %r' % method_frame)
        if self._channel:
            self._channel.close()

    def on_message(self, unused_channel, basic_deliver, properties, body):
        message = json.loads(body)
        self.handle_message(message)
        self.acknowledge_message(delivery_tag=basic_deliver.delivery_tag)

    def handle_message(self, message):
        print 'receive message: %s' % message
        return message

    def acknowledge_message(self, delivery_tag):
        """Acknowledge the message delivery from RabbitMQ by sending a
        Basic.Ack RPC method for the delivery tag.

        :param int delivery_tag: The delivery tag from the Basic.Deliver frame

        """
        LOGGER.info('Acknowledging message %s' % delivery_tag)
        self._channel.basic_ack(delivery_tag)

    def stop_consuming(self):
        """Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.

        """
        if self._channel:
            LOGGER.info('Sending a Basic.Cancel RPC command to RabbitMQ')
            self._channel.basic_cancel(self.on_cancelok, self._consumer_tag)

    def on_cancelok(self, unused_frame):
        """This method is invoked by pika when RabbitMQ acknowledges the
        cancellation of a consumer. At this point we will close the channel.
        This will invoke the on_channel_closed method once the channel has been
        closed, which will in-turn close the connection.

        :param pika.frame.Method unused_frame: The Basic.CancelOk frame

        """
        LOGGER.info('RabbitMQ acknowledged the cancellation of the consumer')
        self.close_channel()

    def close_channel(self):
        """Call to close the channel with RabbitMQ cleanly by issuing the
        Channel.Close RPC command.

        """
        LOGGER.info('Closing the channel')
        self._channel.close()

    def run(self):
        """Run the example consumer by connecting to RabbitMQ and then
        starting the IOLoop to block and allow the SelectConnection to operate.

        """
        self._connection = self.connect()
        self._connection.ioloop.start()

    def stop(self):
        """Cleanly shutdown the connection to RabbitMQ by stopping the consumer
        with RabbitMQ. When RabbitMQ confirms the cancellation, on_cancelok
        will be invoked by pika, which will then closing the channel and
        connection. The IOLoop is started again because this method is invoked
        when CTRL-C is pressed raising a KeyboardInterrupt exception. This
        exception stops the IOLoop which needs to be running for pika to
        communicate with RabbitMQ. All of the commands issued prior to starting
        the IOLoop will be buffered but not processed.

        """
        LOGGER.info('Stopping')
        self._closing = True
        self.stop_consuming()
        self._connection.ioloop.start()
        LOGGER.info('Stopped')

    def close_connection(self):
        """This method closes the connection to RabbitMQ."""
        LOGGER.info('Closing connection')
        self._connection.close()


class Publisher(threading.Thread):
    def __init__(self, conf, queue, exchange=None, exchange_type='topic', binding_key=None):
        super(Publisher, self).__init__()
        self.conf = conf
        self.exchange = queue if exchange is None else exchange
        self.exchange_type = exchange_type
        self.queue = queue
        if binding_key:
            self.routing_key = binding_key
        elif exchange and queue:
            self.routing_key = '%s-%s' % (exchange, queue)
        else:
            self.routing_key = queue
        self._connection = None
        self._channel = None
        self._deliveries = []
        self._acked = 0
        self._nacked = 0
        self._message_number = 0
        self._stopping = False
        self._closing = False
        self.start()

    def __del__(self):
        self.close_channel()
        self.close_connection()

    def connect(self):
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
        LOG.info('Connection opened')
        self.add_on_connection_close_callback()
        self.open_channel()

    def add_on_connection_close_callback(self):
        LOG.info('Adding connection close callback')
        self._connection.add_on_close_callback(self.on_connection_closed)

    def on_connection_closed(self, connection, reply_code, reply_text):
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            LOG.warning('Connection closed, reopening in 5 seconds: (%s) %s' % (reply_code, reply_text))
            self._connection.add_timeout(5, self.reconnect)

    def reconnect(self):
        self._deliveries = []
        self._acked = 0
        self._nacked = 0
        self._message_number = 0
        self._connection.ioloop.stop()
        self._connection = self.connect()
        self._connection.ioloop.start()

    def open_channel(self):
        LOG.info('Creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        LOG.info('Channel opened')
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self.exchange)

    def add_on_channel_close_callback(self):
        LOG.info('Adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reply_code, reply_text):
        LOG.warning('Channel was closed: (%s) %s' % (reply_code, reply_text))
        if not self._closing:
            self._connection.close()

    def setup_exchange(self, exchange_name):
        LOG.info('Declaring exchange %s' % exchange_name)
        self._channel.exchange_declare(self.on_exchange_declareok, exchange_name, self.exchange_type, durable=True)

    def on_exchange_declareok(self, unused_frame):
        LOG.info('Exchange declared')
        self.setup_queue(self.queue)

    def setup_queue(self, queue_name):
        LOG.info('Declaring queue %s' % queue_name)
        self._channel.queue_declare(self.on_queue_declareok, queue_name, durable=True)

    def on_queue_declareok(self, method_frame):
        LOG.info('Binding %s to %s with %s' % (self.exchange, self.queue, self.routing_key))
        self._channel.queue_bind(self.on_bindok, self.queue, self.exchange, self.routing_key)

    def on_bindok(self, unused_frame):
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
        """Invoked by pika when RabbitMQ responds to a Basic.Publish RPC
            command, passing in either a Basic.Ack or Basic.Nack frame with
            the delivery tag of the message that was published. The delivery tag
            is an integer counter indicating the message number that was sent
            on the channel via Basic.Publish. Here we're just doing house keeping
            to keep track of stats and remove message numbers that we expect
            a delivery confirmation of from the list used to keep track of messages
            that are pending confirmation.

            :param pika.frame.Method method_frame: Basic.Ack or Basic.Nack frame

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
        LOG.info('Closing the channel')
        if self._channel:
            self._channel.close()

    def run(self):
        self._connection = self.connect()
        self._connection.ioloop.start()

    def stop(self):
        LOG.info('Stopping')
        self._stopping = True
        self.close_channel()
        self.close_connection()
        self._connection.ioloop.start()
        LOG.info('Stopped')

    def close_connection(self):
        LOG.info('Closing connection')
        self._closing = True
        self._connection.close()


class RpcConsumer(Consumer):
    def __init__(self, conf, queue):
        super(RpcConsumer, self).__init__(conf, queue)

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

    def setup_queue_rpc(self, queue_name):
        LOG.info('Declaring queue %s' % queue_name)
        while not self._channel:
            time.sleep(1)
        self._channel.queue_declare(self.on_queue_declareok_rpc, queue_name, durable=True)

    def on_queue_declareok_rpc(self, method_frame):
        LOG.info('Binding %s to %s with %s' % (self.exchange, self.callback_queue, self.callback_queue))
        self._channel.queue_bind(self.on_bindok_rpc, self.callback_queue, self.exchange, self.callback_queue)

    def on_bindok_rpc(self, unused_frame):
        LOG.info('Queue bound')
        self.start_consuming_rpc()

    def start_consuming_rpc(self):
        LOG.info('Issuing consumer related RPC commands')
        self.add_on_cancel_callback_rpc()
        self._consumer_tag = self._channel.basic_consume(self.on_message, self.callback_queue)

    def add_on_cancel_callback_rpc(self):
        LOG.info('Adding consumer cancellation callback')
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled_rpc)

    def on_consumer_cancelled_rpc(self, method_frame):
        LOG.info('Consumer was cancelled remotely, shutting down: %r' % method_frame)
        if self._channel:
            self._channel.close()

    def on_message(self, channel, method, props, body):
        if props.correlation_id == self.correlation_id:
            self.response = json.loads(body)
        channel.basic_ack(delivery_tag=method.delivery_tag)

    def publish_message(self, message, routing_key=None):
        routing_key = self.routing_key if routing_key is None else routing_key
        if self._stopping:
            return
        while not self._channel:
            time.sleep(1)
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
            self._connection.process_data_events()
        return self.response
