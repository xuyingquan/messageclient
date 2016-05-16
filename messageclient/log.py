#!/usr/bin/python
# -*- coding: utf-8 -*-
#########################################################################
# File Name: log.py
# Author: xuyingquan
# mail: yingquan.xu@shatacloud
# Created Time: Mon May  9 15:54:13 CST 2016
#########################################################################

import logging


def init_logger(appname, path):
    """
    初始化系统日志
    :param logname: str, 日志名称
    :param path: str, 日志路径
    """
    log = logging.getLogger(appname)  # 创建日志
    log.setLevel(logging.DEBUG)

    log_file = logging.FileHandler(path)
    log_file.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s %(process)s %(levelname)s: %(funcName)s: [%(message)s]')
    log_file.setFormatter(formatter)
    log.addHandler(log_file)
    return log


if __name__ == '__main__':
    log = init_logger('test', '/var/log/test.log')
    log.info('hello world')
