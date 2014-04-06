#_*_coding:utf-8 _*_

from django_jdata.exceptions import BaseError 
from django_jdata.utils import return_http_json
from django.shortcuts import HttpResponse

class JdataMiddleware(object):
    def process_exception(self, request, e):
        if isinstance(e,BaseError):
            return e.to_django_response()
        else:
            pass

    def process_response(self, request, response):
        if not isinstance(response, HttpResponse):
            return return_http_json(response)
        else:
            return response

