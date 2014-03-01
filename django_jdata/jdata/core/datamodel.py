#_*_coding:utf-8 _*_
#!/bin/env python

from django_jdata.jdata.core.dbdata import Data
from django_jdata.models import DataObject 
from django_jdata.utils import json
from django_jdata.exceptions import URLParameterError,ObjectNotFound,ObjectConfigError,ObjectConfigTestError,ObjectInitError
import time,base64
import os,imp,urllib2



def QueryDict(path):
    if path.find('?')>0:
        path = path[path.find('?')+1:]
    rst = {}
    for kv in path.split('&'):
        idx = kv.find('=')
        if idx > 0:
            rst[ kv[:idx] ] = kv[idx+1 :]
        else:
            rst[kv] = ''
    return rst



class DataModel(object):
    def __init__(self,objectname=None,path=None, config = {}):
        self.objectname=objectname
        if not self.objectname:
            if path:
                self.objectname=self.get_objectname_by_path(path)
            elif config:
                self.obj = config
            else:
                raise ObjectInitError('parameter objectname | path | config  is required!')
        if not config:
            self._get_config()
        try:
            self._check_config()
        except Exception as e:
            raise ObjectConfigError('object `'+str(self.objectname)+'` configuration error:   '+str(e))
        self._auto_gen_config()
        self.Data = Data(self.objectname, self.DB, self.FIELDS_ALIAS, self.METADB)


    def check_new_config(self):
        testtable = 'jdata_config_test_table'

        try:
            self.Data.createtable(testtable)
            self.Data.droptable(testtable)
            #self.Data.query('drop table '+testtable,iscache=False, tablename = testtable)
        except Exception as e:
            raise ObjectConfigTestError('object `%s` configuration test : Create Table Error ! %s' %(self.objectname, str(e)))
        try:
            self.Data.createtable(testtable)
            self.Data.query('select * from '+testtable, iscache=False, tablename = testtable)
            self.Data.droptable(testtable)
            #self.Data.query('drop table '+testtable, iscache=False, tablename = testtable)
        except:
            raise ObjectConfigTestError('object `'+self.objectname+'` configuration test  error: Replication Lag ? ')
        
            
    def _get_config(self):
        try:_o = DataObject.objects.get(oname = self.objectname)
        except:raise ObjectNotFound('object `'+self.objectname+'` not found.')
        self.obj = json.loads(_o.conf)
    
    def _check_config(self):
        self.objectname = self.obj['KEY']
        self.FIELDS_DISPNAME = {}
        self.DB = self.obj['DB']
        _s = self.DB['mysql']['table_split_idx']
        self.DB['mysql']['table_split_idx'] = int(_s)
        #mysqlconfig = self.DB['mysql']['writerurl']
        _METADB = self.obj['METADB']
        self.METADB = []
        for i in _METADB:
            self.METADB.append((i['name'],i['lambda'],i['alias']))
            self.FIELDS_DISPNAME[i['name']] = i['alias']
        _FIELDS_ALIAS = self.obj['FIELDS_ALIAS']
        self.FIELDS_ALIAS={}
        for i in _FIELDS_ALIAS:
            self.FIELDS_ALIAS[i['name']] = (i['lambda'],i['alias'])
            self.FIELDS_DISPNAME[i['name']] = i['alias']
        self.NAME = self.obj['NAME']
        

    def _auto_gen_config(self):
        #self.URL_ALIAS = self.obj.get('URL_ALIAS',{'s':'starttime','e':'endtime'})
        self.SHOW_IN_QUERY = self.obj.get('SHOW_IN_QUERY',False)
        _ALLOWIPS = self.obj.get('ALLOWIPS',[])
        self.ALLOWIPS = [i['ip'] for i in _ALLOWIPS]

        #generel METADB_CHART
        str_fields =  [ i[0] for i in  list(self.METADB) if 'char' in i[1]]
        str_fields.remove(self.FIELDS_ALIAS['timeline'][0])
        int_fields = self.FIELDS_ALIAS.keys() + [ i[0] for i in  list(self.METADB) if ('int' in i[1] or 'float' in i[1])]
        int_fields.remove('timeline') 
        self.METADB_CHART = {}
        for i in str_fields:
            self.METADB_CHART[i] = {'p':i,'name':(self.FIELDS_DISPNAME.get(i) or i),'pageby':True,'filter':True,'fields':False}
        for i in int_fields:
            self.METADB_CHART[i] = {'p':i,'name':(self.FIELDS_DISPNAME.get(i) or i),'pageby':False,'filter':False,'fields':True}
        
        
        
    def get_objectname_by_path(self,path):
        url_q = QueryDict(path)
        if url_q.has_key('_o'):
            return url_q['_o']
        if url_q.has_key('_sql'):
            return self.get_query_dict_by_sql(url_q['_sql'])['_o']
        raise URLParameterError('parameter  `_o` is required.') 

  
    def get_query_dict_by_sql(self, base64_sql):
        src_sql = base64.b64decode(base64_sql)
        sql = src_sql.lower().strip().replace('\n', ' ').replace('\t', ' ')
        while True:
            if sql.find('  ')>0:
                sql = sql.replace('  ',' ')
            else:
                break
        if not sql.startswith('select'):
            raise URLParameterError('You have an error in your SQL syntax: [%s]' %src_sql)

        # 去掉select关键字
        sql = sql[6:]
        
        try:
            (sql_1, sql_2) = sql.split(' from ')
        except ValueError:
            raise URLParameterError('You have an error in your SQL syntax: no `from` found. [%s]' %src_sql)
        
        # 拿到查询的字段 _fields  sql_1=' _tstep:5 _refresh field1,field2 '
        fields = [i.strip() for i in sql_1.split(',') if i.strip()]
        _fields = [fields[0].split()[-1],] + fields[1:]
                
        # 获取其他jdata参数 
        jdata_paras  = fields[0].split()[:-1]
        

        # 获取表名:_o   
        # where后面的过滤条件: sql_w --> _filters
        # group by后面的分组条件: sql_g --> _pageby

        try:
            (sql_o_w, sql_g) = sql_2.split(' group by ')
            
        except ValueError:
            sql_o_w = sql_2
            sql_g = ''
        
        try:
            _o, sql_w = sql_o_w.split(' where ')
        except ValueError:
            _o = sql_o_w
            sql_w = ''

        #  _filters
        _filters = []
        _s = ''
        _e = ''
        for i in  sql_w.split(' and '):
            i = i.strip()
            if not i:
                continue
            if i.startswith('ptime'):
                if i.find('>=')>0:
                    _s = i.split('>=')[1]
                elif i.find('>')>0:
                    _s = i.split('>')[1]
                elif i.find('<=')>0:
                    _e = i.split('<=')[1]
                elif i.find('<')>0:
                    _e = i.split('<')[1]
                elif i.find('=')>0:
                    _s = i.split('=')[1]
                    _e = i.split('=')[1]
                else:
                    raise URLParameterError('You have an error in your SQL syntax: `%s` [%s]' %(i,src_sql))
            else:
                _filters.append(i)
        _s = _s.strip()
        _e = _e.strip()

        # _pageby 
        _pageby = [i for i in sql_g.split(',') if i ]

        query_dict =  {'_o':_o,
                '_fields':_fields,
                '_filters':_filters,
                '_pageby':_pageby,
                }

        # _s & _e
        if _s:
            query_dict['_s'] = _s
        if _e:
            query_dict['_e'] = _e
    
        # set jdata_paras
        for i in jdata_paras:
            if i.find(':')>0:
                (k, v) = i.split(':')
                query_dict[k] = v
            else:
                query_dict[i] = ''
        
        return query_dict

 
    def get_query_dict(self,path):
        path = urllib2.unquote(path)
        items=QueryDict(path).items()
        query_dict={}
        query_dict['_pageby']=[]
        query_dict['_fields']=[]
        query_dict['_filters']=[]
        for i in items:
            if i[0] == '_o' :
                query_dict['_o']=i[1]
        for i in items:
            k = i[0][i[0].find('?')+1:]
            if k =='_pageby':
                if len(i[1])>0:
                    for f in i[1].split(','):
                        query_dict['_pageby'].append(f)
            elif k =='_fields':
                for f in i[1].split(','):
                    query_dict['_fields'].append(f)
            elif k.find('_') == 0:
                query_dict[k] = i[1]
            elif k == '':
                continue
            else:
                if i[1]=='':
                    query_dict['_filters'].append(k)
                elif i[1].find('%')>=0:
                    query_dict['_filters'].append(k+' like "'+i[1]+'"')
                else:
                    query_dict['_filters'].append(k+'="'+i[1]+'"')
        if query_dict.has_key('_sql'):
            query_dict =  self.get_query_dict_by_sql(query_dict['_sql'])
                
        if query_dict.has_key('_nocheck') or query_dict.has_key('_nourlcheck'):
            return query_dict
        if not query_dict.has_key('_o'):
            raise URLParameterError('parameter  `_o` is required.')
        if len(query_dict['_fields'])==0:
            raise URLParameterError('parameter  `_fields` is required.')
        self._check_query_dict(query_dict)
        return query_dict



    def _check_query_dict(self, query_dict):
        for i in query_dict['_fields']:
            keys = self.FIELDS_ALIAS.keys() + self.Data.cols
            if i not in keys:
                raise URLParameterError('Unknown field `'+i+'`  , expecting one of: "'+', '.join(keys)+'"')
        for i in query_dict['_filters'] + query_dict['_pageby']:
            i = i.split()[0].split('=')[0]
            if not (i in self.Data.cols):
                raise URLParameterError('Unknown field `'+i+'` , expecting one of: "'+', '.join(self.Data.cols)+'"')

        # tstep有效性检查
        # 默认5分钟，必须填整数，单位分钟
        # 如果长度超过了分表粒度，则报错
        tstep = query_dict.get('_tstep',5)
        try:
            tstep = int(tstep)
        except ValueError:
            raise URLParameterError('The value of parameter `_tstep` must integer')

        split_idx = self.DB['mysql']['table_split_idx']
        if split_idx <= 8:  # 分表粒度为day month year的，最大tstep是1 day
            max_tstep = 60*24
        elif split_idx == 10:  # 分表粒度为 Hour 的，最大tstep是1 hour
            max_tstep = 60
        elif split_idx == 12: #分表粒度为 minute的，最大tstep是1 minute
            max_tstep = 1;
        if int(tstep) > max_tstep:
            raise URLParameterError('`_tstep` must less than %s minute ,default is 5(min), current value is %s' %(max_tstep, tstep))

