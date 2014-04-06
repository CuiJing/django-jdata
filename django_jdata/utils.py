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

#project_dir  = settings.BASE_DIR
try:
    JDATA_TEMPDIR = settings.JDATA_TEMPDIR
except:
    JDATA_TEMPDIR = '/tmp/jdata/'

def json_encode_decimal(obj):
	if isinstance(obj, decimal.Decimal):
		return '%s' %obj
	if isinstance(obj, datetime.datetime):
		return str(obj)
    
	raise TypeError(repr(obj) + " is not JSON serializable")


def return_http_json(rst):
	r = HttpResponse()
	r.write(json.dumps(rst,default=json_encode_decimal))
	return r
	

def log(s, level = 0):   #Error:0,  Info:1,  Debug:2
    if settings.DEBUG:
        log_level = 2
    else:
        log_level = 0
    if level <= log_level:
        sys.stdout.write(time.strftime('%Y-%m-%d %H:%M:%S ')+": "+s+'\n')
    
