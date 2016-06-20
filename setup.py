# -*- coding: utf-8 -*-
#########################################################################
# File Name: setup.py
# Author: xuyingquan
# mail: yingquan.xu@shatacloud
# Created Time: Mon May 16 14:39:37 CST 2016
#########################################################################
#!/usr/bin/python


from distutils.core import setup
import os

name_prefix = os.environ.get('NAME_PREFIX', '')
software_name = os.environ.get('SOFTWARE_NAME', '')
version = os.environ.get('VERSION', '')
batch_num = os.environ.get('BUILD_NUMBER', '0')


if software_name != '':
    name = name_prefix + software_name
else:
    name = "shata-messageclient"

if version != '':
    version = '%s.%s' % (version, batch_num)
else:
    version = '1.3.2'

setup(
    name=name,
    version=version,
    url="http://www.shatacloud.com/",
    packages=["messageclient", "messageclient.rabbitmq_driver"],
    scripts=[]
)
