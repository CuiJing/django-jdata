# -*- coding:utf-8 -*-   
from django.shortcuts import render_to_response

from django_jdata.jdata.core.datamodel import DataModel
from django_jdata.jdata.core.dataconfig import get_tablelocation
from django_jdata.exceptions import * 
from django_jdata.utils import JDATA_TEMPDIR, log
import time

def get(request, linechart = False):
    path = request.get_full_path()
    if linechart:
        path = path + '&_linechart'
    DM = DataModel(path=path)
    client = request.META.get('REMOTE_ADDR','')
    if DM.ALLOWIPS and (client not in DM.ALLOWIPS):
        raise AccessDenied('Your IP `'+client+'` is not trusted  for `'+DM.objectname+'`.')
    rst=DM.Data.get(DM.get_query_dict(path))
    return rst


def getsettings(request):
    """
    setting方法返回目前各数据对象的配置项
    e.g. http://jdata.domain/api/setting?_o=slowspeed1h
    """
    obj = request.GET.get('_o','')
    if not obj:
        raise URLParameterError('parameter  `_o` is required.')
    DM = DataModel(obj)
    rst = DM.__dict__
    rst.pop('Data')
    return rst

def getquerydict(request):
    """
    getquerydict方法返回根据url解析出来的统一格式的请求参数
    e.g. http://jdata.domain/api/getquerydict?_o=feedbacklog&_tstep=5&_dataclean=0&_fields=count&s=201203051555&e=201203061555&_lines=fields
    """
    path=request.get_full_path()
    DM = DataModel(path = path)
    return DM.get_query_dict(path)

def gettables(request):
    """
    gettables 方法将返回此数据的表
    e.g.  http://jdata.domain/api/gettables?_o=cache_customer_bw
    """
    obj = request.GET.get('_o','')
    if not obj:
        raise URLParameterError('parameter  `_o` is required.')
    rst = get_tablelocation(obj)
    x = [ {'ObjectName':obj, 
            'TableName':i.tname,
            'Created':i.created} for i in rst ]
    #rst = DM.Data.queryexecute('show table status')['Data']
    #x = [ {'TableName':i[0],'Engine': i[1],'Rows':i[4],'DataSize':i[6],'IndexSize':i[8],'Created':i[11] } for i in rst ]
    return x 

def getlatesttime(request):
    '''
    getlatesttime 
    '''
    obj = request.GET.get('_o','')
    if not obj:
        raise URLParameterError('parameter  `_o` is required.')
    DM = DataModel(objectname = obj)
    return DM.Data.latest_timeline()

    
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
    return {'Droped':num}
    

from os import mkdir,remove
from os.path import isdir,join

def put(request):
    """
    上传文件，做入库处理  POST 方法
    参数：
    file=@filename   本地文件
    _o=cdnbw         数据object名字
    _s=201203201340   这份数据的时间戳，请注意该文件中的数据时间跨度不得超过定义的拆表时间区间（table_split）
    _t=,             数据的分隔符，默认空格
    data_exists_action  数据如果存在（主键冲突）的处理方法，支持3种模式：    ignore (Default) | replace | append
                        ignore:    如果发现数据已经存在，则忽略；不存在的数据可以成功导入
                        replace:   如果已存在，则替换掉旧数据
                        append:    如果已存在，则往上累加（除主键外的其他字段，当存在百分比类似的数据时不适合使用append，除非确认可以对这些字段做累加）

    e.g. curl  -F file=@filename -F _o=cdnbw -F _s=201203201340 -F '_t=,' -F data_exists_action=replace http://.../jdata/data/put

    """
    if not isdir(JDATA_TEMPDIR):mkdir(JDATA_TEMPDIR)
    if request.method == 'POST':
        if request.META.has_key('HTTP_X_FORWARDED_FOR'):  
            client =  request.META['HTTP_X_FORWARDED_FOR']  
        else:  
            client = request.META['REMOTE_ADDR'] 
        file = request.FILES['file']
        filename = file.name
        try:obj = request.POST['_o']
        except:raise UploadDataParameterError("parameter `_o` is required")
        DM = DataModel(obj)
        try:timeline = request.POST['_s']
        except:raise UploadDataParameterError("parameter `_s` is required")
        fields_terminated = request.POST.get('_t',' ')

        objdir = join(JDATA_TEMPDIR,obj)
        if not isdir(objdir):
            mkdir(objdir)
        now = time.strftime('%Y%m%d%H%M%S',time.localtime())
        tmpfile = join(objdir,now+'_'+timeline+'_'+filename)
        f = open(tmpfile,'wb')
        f.write(file.read())
        f.close()
        data_exists_action = request.POST.get('data_exists_action','ignore')
        t1 = time.time()

        if data_exists_action in('ignore','replace', 'append'):
            tablename = DM.Data.gettablename(timeline)
            if data_exists_action in ('replace', 'append'):
                if not DM.Data.pk:
                    raise ObjectPKNotFound('No PK Found For this Data `%s` ,so you can not use parameter `%s`' %(obj, data_exists_action))
            rst = DM.Data.loadfile2db(tmpfile, tablename, fields_terminated, data_exists_action)
            log('upload: %s %s [%s] %ss %s %s' %(client, obj, data_exists_action, int(time.time()-t1),tmpfile,rst))
            try: 
                remove(tmpfile)
            except:
                log('remove resultfile Failed: %s' %tmpfile)
            return rst
        else:
            raise UploadDataParameterError(" Unknown value `"+data_exists_action+"` for data_exists_action")
    else:
        raise UploadDataMethodError("""Please POST your data like this: curl  -F file=@filename -F _o=cdnbw -F _s=201203201340 -F '_t= '  http://jdata.domain/api/uploadresultfile""")
        
