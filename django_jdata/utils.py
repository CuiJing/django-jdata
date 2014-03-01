#_*_coding:utf-8 _*_
# Create your views here.
from django.http import Http404, HttpResponse,HttpResponseRedirect
#from django.utils import simplejson
from django.conf import settings 
from django.core.cache import cache as sqlcache
import decimal
import datetime
import sys,time
import json

project_dir  = settings.BASE_DIR
DMC_MASTER_W = settings.JDATA_DMC_MASTER_W
DMC_MASTER_R = settings.JDATA_DMC_MASTER_R

def json_encode_decimal(obj):
	if isinstance(obj, decimal.Decimal):
		return str(obj)
	if isinstance(obj, datetime.datetime):
		return str(obj)
    
	raise TypeError(repr(obj) + " is not JSON serializable")


def return_http_json(rst):
	r = HttpResponse()
	r.write(json.dumps(rst,default=json_encode_decimal))
	return r
	

def log(s, level = 0):  # 0:INFO  1: DEBUG
    if level <= settings.JDATA_LOG_LEVEL:
        sys.stdout.write(time.strftime('%Y-%m-%d %H:%M:%S ')+": "+s+'\n')
    
