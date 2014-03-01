# -*- coding:utf-8 -*-   
from django.shortcuts import render_to_response

# Create your views here.
from django_jdata.jdata.core.datamodel import DataModel

def get(request):
    path=request.get_full_path()
    DM=DataModel(path=path)
    client = request.META.get('REMOTE_ADDR','')
    if DM.ALLOWIPS and (client not in DM.ALLOWIPS):
        raise AccessDenied('Your IP `'+client+'` is not trusted  for `'+DM.objectname+'`.')
    rst=DM.Data.get(DM.get_query_dict(path))
    return return_http_json(rst)


