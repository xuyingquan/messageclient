#!/usr/bin/python
# -*- coding: utf-8 -*-
#########################################################################
# File Name: util.py
# Author: xuyingquan
# mail: yingquan.xu@shatacloud
# Created Time: Mon May  9 15:54:13 CST 2016
#########################################################################

import logging
import ConfigParser
import os
import sys
import signal
import collections


def init_logger(appname, path):
    """
    初始化系统日志
    :param logname: str, 日志名称
    :param path: str, 日志路径
    """
    if not os.path.exists(path):
        path = path.strip()
        dirname = os.path.dirname(path)
        basename = os.path.basename(path)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        os.chdir(dirname)
        os.mknod(basename)
    log = logging.getLogger(appname)  # 创建日志
    log.setLevel(logging.DEBUG)

    log_file = logging.FileHandler(path)
    log_file.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s %(process)s %(levelname)s: %(funcName)s: [%(message)s]')
    log_file.setFormatter(formatter)
    log.addHandler(log_file)
    return log


def load_config(path):
    conf = ConfigParser.ConfigParser()
    conf.read(path)
    return conf


def daemonize(home_dir='.', umask=022, stdin=os.devnull, stdout=os.devnull, stderr=os.devnull):
    """ 初始化当前进程为后台daemon进程

    """
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


def timeout_handler(signum, callback):
    raise AssertionError


def timeout_decorator(fn):
    """ 超时装饰器

    """
    def _decorator(message, mode='rpc', timeout=-1):
        try:
            signal.signal(signal.SIGALRM, timeout_handler)
            if timeout == -1:
                signal.alarm(1000000)
            else:
                signal.alarm(timeout)
        except AssertionError:
            print 'timeout return'
        result = fn(message, mode)
        signal.alarm(0)
        return result
    return _decorator


def is_callable(fn):
    """ 判断一个函数是否是可调用

    """
    return isinstance(fn, collections.Callable)


if __name__ == '__main__':
    log = init_logger('test', '/var/log/iaas.log')
    log.info('hello world')
