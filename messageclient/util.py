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


if __name__ == '__main__':
    log = init_logger('test', '/var/log/iaas.log')
    log.info('hello world')
