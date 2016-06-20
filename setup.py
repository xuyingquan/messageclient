# -*- coding: utf-8 -*-
#########################################################################
# File Name: setup.py
# Author: xuyingquan
# mail: yingquan.xu@shatacloud
# Created Time: Mon May 16 14:39:37 CST 2016
#########################################################################
#!/usr/bin/python


from distutils.core import setup


SOFTWARE_NAME = "shata-messageclient"
VERSION = '1.3.2'
URL = "http://www.shatacloud.com/"


setup(
    name=SOFTWARE_NAME,
    version=VERSION,
    url=URL,
    packages=["messageclient", "messageclient.rabbitmq_driver"],
    scripts=[]
)
