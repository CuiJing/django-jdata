#!/usr/bin/python
#_*_coding:utf-8 _*_
from django.test import Client

class TestViews(unittest.TestCase):
    def __init__(self):
        self.c = Client()

    def test_ping(self):
        res = self.c.get('/data/q')
        res.
