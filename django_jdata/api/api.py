#!/usr/bin/python
# -*- coding: utf-8 -*-
# Create your views here.
from .jdata.core.datamodel import DataModel
from .utils import return_http_json
from .exceptions import URLParameterError
import os,sys,time


def setting(request):
    """
    setting方法返回目前各数据对象的配置项
    e.g. http://jdata.domain/api/setting?_o=slowspeed1h
    """
    obj = request.GET.get('_o','')
    opt = request.GET.get('opt','')
    if not obj:
        raise URLParameterError('parameter  `_o` is required.')
    DM = DataModel(obj)
    rst = DM.__dict__
    rst.pop('Data')
    #rst.pop('METADB_CHART')
    #rst.pop('URL_ALIAS')
    #rst.pop('obj')
    '''
    try:
        w = rst['DB']['mysql']['writerurl']
        rst['DB']['mysql']['writerurl'] = w.split('@')[1]
        r = rst['DB']['mysql']['readerurl']
        rst['DB']['mysql']['readerurl'] = r.split('@')[1]
    except:
        pass
    '''
    
    if opt:
        try:return return_http_json(rst[opt])
        except:raise URLParameterError('Data `'+obj+'` has not option: `'+opt+'`')
    else:
        return return_http_json(rst)

def getquerydict(request):
    """
    getquerydict方法返回根据url解析出来的统一格式的请求参数
    e.g. http://jdata.domain/api/getquerydict?_o=feedbacklog&_tstep=5&_dataclean=0&_fields=count&s=201203051555&e=201203061555&_lines=fields
    """
    path=request.get_full_path()
    DM = DataModel(path = path)
    return return_http_json(DM.get_query_dict(path))

def gettables(request):
    """
    gettables 方法将返回此数据的表
    e.g.  http://jdata.domain/api/gettables?_o=cache_customer_bw
    """
    obj = request.GET.get('_o','')
    if not obj:
        raise URLParameterError('parameter  `_o` is required.')
    DM = DataModel(obj)
    if DM.Data.dmc:
        sql = 'select oname, tname, created from jdata_dmc_tables where oname = "%s"' %obj
        rst = DM.Data.query_master(sql)
        x = [ {'ObjectName':i[0], 'TableName':i[1],'Created':i[2] } for i in rst ]
    else:
        rst = DM.Data.queryexecute('show table status')['Data']
        x = [ {'TableName':i[0],'Engine': i[1],'Rows':i[4],'DataSize':i[6],'IndexSize':i[8],'Created':i[11] } for i in rst ]
    return return_http_json(x)
    
def droptables(request):
    """
    droptables  删除指定MySQL Tables
    e.g. http://jdata.domain/api/droptable?_o=cache_customer_bw&_tables=t1,t2,t3
    
    """
    obj = request.GET.get('_o','')
    tbls = request.GET.get('_tables','')
    if not obj:
        raise URLParameterError('parameter  `_o` is required.')
    if not tbls:
        raise URLParameterError('parameter  `_tables` is required.')
    DM = DataModel(obj)
    num = 0
    for i in tbls.split(','):
        if i:
            try:
                t = DM.Data.droptable(tablename = i)
                num += 1
            except:
                pass
    return return_http_json({'Droped':num})
    
