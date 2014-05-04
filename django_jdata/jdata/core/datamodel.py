#_*_coding:utf-8 _*_
#!/bin/env python

import time,base64
import os,imp,urllib2

from django_jdata.utils import json
from django_jdata.exceptions import URLParameterError,ObjectConfigError,ObjectConfigTestError,ObjectInitError

from django_jdata.jdata.core.dbdata import Data

from django_jdata.jdata.core.dataconfig import get_jdata_config

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
    def __init__(self, objectname=None, path=None, config = {}):
        self.objectname=objectname
        if not self.objectname:
            if path:
                self.objectname=self.get_objectname_by_path(path)
            elif config:
                self.obj = config
            else:
                raise ObjectInitError('parameter objectname | path | config  is required!')
        if not config:
            config = get_jdata_config(self.objectname)
        try:
            self._auto_gen_config(config)
        except Exception as e:
            raise e

        #find non pk fields
        _idx = 0
        nonpk = []
        for i in self.FIELDS:
            if i['field'] not in self.PK:
                nonpk.append((i['field'], _idx))
            _idx += 1

        ##
        self.Data = Data(objectname = self.objectname, 
                         fields = self.FIELDS,
                         fields_alias_d = self.FIELDS_ALIAS_D, 
                         fields_dispname = self.FIELDS_DISPNAME,
                         create_sql = self.CREATE_SQL,
                         #nonpk = list(set(self.FIELDS_D.keys()) - set(self.PK)),
                         nonpk = nonpk,
                         table_split_idx = self.TABLE_SPLIT_IDX,
                         )

    
    def _auto_gen_config(self, config):
        for (attr, value) in config.items():
            self.__setattr__(attr, value)
        
        self.objectname = self.NAME

        #generate FIELDS_DISPNAME from METADB and FIELDS_DISPNAME
        #dict for fields
        self.FIELDS_D = {}
        #dict for fields_alias
        self.FIELDS_ALIAS_D = {}
        self.FIELDS_DISPNAME = {}
        for i in self.FIELDS:
            self.FIELDS_D[i['field']] = (i['datatype'], i['comment'])
            self.FIELDS_DISPNAME[i['field']] = i['comment'] or i['field']
        for i in self.FIELDS_ALIAS:
            self.FIELDS_ALIAS_D[i['field_alias']] = (i['expression'], i['comment'])
            self.FIELDS_DISPNAME[i['field_alias']] = i['comment'] or i['field_alias']
        
        '''
        #generate METADB_CHART
        str_fields =  [ i['field'] 
                            for i in  self.FIELDS 
                            if 'char' in i['datatype'].lower()
                        ]
        #str_fields.remove(self.FIELDS_ALIAS['timeline'][0])
        '''
        int_fields = ([i['field_alias'] 
                        for i in self.FIELDS_ALIAS] 
                        + 
                     [ i['field'] 
                        for i in self.FIELDS 
                        if ('int' in i['datatype'].lower() 
                            or 'float' in i['datatype'].lower()
                          )
                     ])
        '''
        #int_fields.remove('timeline') 
        self.CHART_LINE = {}
        for i in str_fields:
            self.CHART_LINE[i] = {'p':i,'name':(self.FIELDS_DISPNAME[i]),'pageby':True,'filter':True,'fields':False}
        for i in int_fields:
            self.CHART_LINE[i] = {'p':i,'name':(self.FIELDS_DISPNAME[i]),'pageby':False,'filter':False,'fields':True}
        '''
        
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

        #check _fields
        int_fields = ([i['field_alias'] 
                        for i in self.FIELDS_ALIAS] 
                        + 
                     [ i['field'] 
                        for i in self.FIELDS 
                        if ('int' in i['datatype'].lower() 
                            or 'float' in i['datatype'].lower()
                          )
                     ])
        int_fields = list(set(int_fields))
        for i in query_dict['_fields']:
            if i not in int_fields:
                raise URLParameterError('Unknown field `%s`  , expecting one of: "%s"' %(i, ', '.join(int_fields)))
            
        # check _filters and _pageby
        for i in query_dict['_filters'] + query_dict['_pageby']:
            i = i.split()[0].split('=')[0]
            if not (i in self.FIELDS_D.keys()):
                raise URLParameterError('Unknown field `%s` , expecting one of: "%s"' %(i, ', '.join(self.FIELDS_D.keys())))


        # tstep有效性检查
        # 默认5分钟，必须填整数，单位分钟
        # 如果长度超过了分表粒度，则报错
        tstep = query_dict.get('_tstep',5)
        try:
            tstep = int(tstep)
        except ValueError:
            raise URLParameterError('The value of parameter `_tstep` must integer')


        if self.TABLE_SPLIT_IDX <= 8:  # 分表粒度为day month year的，最大tstep是1 day
            max_tstep = 60*24
        elif self.TABLE_SPLIT_IDX == 10:  # 分表粒度为 Hour 的，最大tstep是1 hour
            max_tstep = 60
        elif self.TABLE_SPLIT_IDX == 12: #分表粒度为 minute的，最大tstep是1 minute
            max_tstep = 1;
        if int(tstep) > max_tstep:
            raise URLParameterError('`_tstep` must less than %s minute ,default is 5(min), current value is %s' %(max_tstep, self.TABLE_SPLIT_IDX))

