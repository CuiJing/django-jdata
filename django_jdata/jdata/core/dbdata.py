#_*_coding:utf-8 _*_
import MySQLdb
from MySQLdb import OperationalError,ProgrammingError,Warning
import re,sys
import time,hashlib,os,traceback
from threading import Thread,Lock
from dateutil import rrule
from dateutil.parser import parse

from django_jdata.jdata.plugins.filter import FilterGroupLimit, FilterNoise, FillGroupbyWithTime
from django_jdata.exceptions import *
from django_jdata.utils import log,sqlcache 

from django_jdata.jdata.core.dataconfig import *

'''
a class in database layer  for Datamodel
'''

class Data(object):
    def __init__(self, **kwargs):
        '''
        self.objectname = objectname
        self.fields = fields
        self.fields_alias_d = fields_alias_d
        self.fields_dispname = fields_dispname
        self.create_sql = create_sql
        self.nonpk = nonpk
        self.table_split_idx = table_split_idx
        '''
        for (k,v) in kwargs.items():
            self.__setattr__(k, v)

        self.timefield = self.fields[0]['field']
        #self.nonpk = list(set(self.fields_d.keys()) - set(self.pk))
        self.dmc_balance_by = 'Round-Robin' # 'Load or Round-Robin'

        self.multithreadresult = ()
        self.rowsfromdisk = 0
        self.rowsfrommem = 0
        self.querys_notfound = 0
        self.querys_frommem = 0
        self.querys_fromdisk = 0
        self.multithreadlock=Lock()

    def dmc_add_newtable(self, tablename):
        if self.dmc_balance_by == 'Load':
            next_mynode_id = get_next_mynode_load(self.objectname)
        else:
            next_mynode_id = get_next_mynode_roundrobin(self.objectname)
        add_tablelocation(self.objectname, next_mynode_id, tablename)

    def dmc_remove_table(self, tablename):
        remove_tablelocation(self.objectname, tablename)
    
    def conn(self, readonly = False, tablename = ''):
        mysql_conf = get_mynode_by_tablename(self.objectname, tablename, readonly)
        return self._conn(mysql_conf)

    def _conn(self, mysql_conf_str):
        t = re.search('^(?P<user>([0-9a-zA-Z-_\.]*?))/[\'\"]?(?P<passwd>([\s\S]*?))[\'\"]?@(?P<host>([0-9a-zA-Z\._-]*?)):(?P<port>(\d*?))/(?P<db>([0-9a-zA-Z-_])*?)$',mysql_conf_str)
        d = t.groupdict()

        c = MySQLdb.connect(host   = d['host'],
                            port   = int(d['port']),
                            user   = d['user'],
                            passwd = d['passwd'],
                            db     = d['db'],
                            )
        c.set_character_set('utf8')
        c.autocommit=1
        return c

    def queryexecute(self,sql, tablename = ''):
        if sql.lower().find('select') == 0:
            conn = self.conn(readonly = True, tablename = tablename)
            log('R: %s' %sql, 1)
        else:
            conn = self.conn(readonly = False, tablename = tablename )
            log('W: %s' %sql, 1)
        c = conn.cursor()
        c.execute('set default_storage_engine = "myisam"')
        rows_examined = 0
        try:
            try:
                c.execute(sql)
            except OperationalError , e:
                if e[0] == 1203:
                    log('1203 Error max_user_connections, retry...', 1)
                    time.sleep(0.01)
                    c.execute(sql)
                elif e[0] == 1050:
                    raise TableAlreadyExists(e[1])
                else:
                    raise e
            except ProgrammingError , e:
                #if sql.lower().find('select') == 0 and e[0] == 1146:
                if e[0] == 1146:
                    raise TableNotExists('Table not exists: '+e[1])
                else:
                    raise e
                
            if sql.lower().find('load ') == 0:
                d = conn.info()
                if d:  #'Records: 10  Deleted: 0  Skipped: 0  Warnings: 0'
                    d = dict([i.split(':') for i in d.split('  ')])
                    d = dict([(i[0], int(i[1])) for i in d.items()])
                else:
                    d = {'Records':c.rowcount, 'Deleted': 0, 'Skipped': 0, 'Warnings': 0}
            else:
                d = c.fetchall()
                c.execute('SHOW STATUS LIKE "Handler_read%"')
                stat = c.fetchall()
                for i in stat:
                    if i[0] in ('Handler_read_next','Handler_read_rnd_next'):
                        rows_examined += int(i[1])
        finally:
            c.close()
            conn.close()
        return {'Data':d, 'ProcessedRowsDisk':rows_examined, 'ProcessedRowsMem':0}

    def query_append(self, mycursor, tablename ,values):
        i_sql = 'insert into ' +tablename+ ' values ("' + '","'.join(values) + '")'
        u_sql = ','.join( [field + '= '+ field + '+' + values[idx]  for (field, idx) in self.nonpk] )
        sql = '%s on duplicate key update %s' %(i_sql, u_sql)
        mycursor.execute(sql)
        row = mycursor.rowcount
        if row == 1:
            return 'insert'
        elif row == 2:
            return 'update'
        
    def query(self,s,iscache=True,refresh=False,tablename = ''):
        if not iscache:
            return self.queryexecute(s, tablename)
        m=hashlib.md5()
        m.update(s)
        ky=m.hexdigest()
        ky = 'jd_'+ky

        if not refresh:
            v = sqlcache.get(ky)
            if v:
                log('CACHE HIT:%s ProcessedRowsMem:%s' %(tablename, v['ProcessedRowsMem']))
                return v
            else:
                info = 'CACHE MISS:'
        else:
            info = 'CACHE REFRESH:'
        try:
            rst = self.queryexecute(s, tablename)
        except TableNotExists,e:
            log('%s %s' %(info, e), 1)
            return {'Data':(),'ProcessedRowsMem':0,'ProcessedRowsDisk':0}
        except TableNotExistsInMetaDB,e:
            log('%s %s' %(info, e), 1)
            return {'Data':(),'ProcessedRowsMem':0,'ProcessedRowsDisk':0}
        log('%s:%s ProcessedRowsDisk:%s' %(info, tablename, rst['ProcessedRowsDisk']), 1)
        mem_rst = {}
        mem_rst['Data'] = rst['Data']
        mem_rst['ProcessedRowsMem'] = rst['ProcessedRowsDisk']
        mem_rst['ProcessedRowsDisk'] = 0
        sqlcache.set(ky,mem_rst,60*60*24)
        return rst


    def multithreadquery(self,sql,step_minute,pageby,refresh,tablename):
        rst=self.query(sql,refresh=refresh, tablename = tablename)
        t = rst['Data']
        rows_disk = rst['ProcessedRowsDisk']
        rows_mem = rst['ProcessedRowsMem']
        self.multithreadlock.acquire()
        self.multithreadresult = self.multithreadresult + t
        self.rowsfromdisk = self.rowsfromdisk + rows_disk
        self.rowsfrommem = self.rowsfrommem + rows_mem
        if rows_disk == 0:
            if rows_mem == 0:
                self.querys_notfound += 1
            else:
                self.querys_frommem += 1
        else:
            self.querys_fromdisk += 1
        
        if str(step_minute) == '0' and self.multithreadresult:
            if pageby:
                pagebynum = len(pageby)
                x={}
                for i in self.multithreadresult:
                    k = tuple(i[:pagebynum])
                    if x.get(k):
                        v = [ x[k][-1-j] + i[-1-j]  for j in range(len(x[k])-pagebynum)]
                        v = list(k) + v
                        x[k] = v
                    else:
                        x[k] = i
                self.multithreadresult = tuple(x.values())
            else:
                x=[]
                for i in self.multithreadresult[0]:
                    x.append(0)
                for i in  self.multithreadresult:
                    try:
                        x = [ x[j]+i[j] for j in range(len(i)) ]
                    except TypeError:
                        pass
                self.multithreadresult = (x,)
        self.multithreadlock.release()

    
    def gettablename(self,starttime=None):
        split_idx = self.table_split_idx
        if starttime:
            if len(starttime) < split_idx:
                raise SpecifiedTimeTooShort('The time `'+starttime+'` is too short(<'+str(split_idx)+') to confirm the Table')
            return self.objectname + starttime[:split_idx]
        else:
            return self.objectname + time.strftime('%Y%m%d%H%i',time.localtime())[:split_idx]

    
    def createtable(self,tablename):
        sql = 'select 1 from %s where 1=2' %tablename
        try:
            self.query(sql, iscache=False, tablename=tablename)
            hastable = True
        except:
            hastable = False
        if hastable:
            raise TableAlreadyExists('Table %s already exists!' %tablename)
        
        rename_sql = 'alter table %s rename to %s' %(self.objectname, tablename)
        try:
            self.query(self.create_sql,iscache=False,tablename = tablename)
        except TableNotExistsInMetaDB:
            self.dmc_add_newtable(tablename)
            self.query(self.create_sql,iscache=False,tablename = tablename)
        self.query(rename_sql,iscache=False,tablename = tablename)


    def droptable(self, tablename):
        dropsql = 'drop table '+tablename
        self.query(dropsql,iscache=False,tablename = tablename)
        self.dmc_remove_table(tablename)


    def loadfile2db(self,fname,tablename,fields_terminated=" ",load_mode = ''):
        """
        load_mode : append | replace | igonre(default)
        """
        try:
            self.createtable(tablename)
        except TableAlreadyExists as e:
            pass
        if load_mode == 'append':
            # Load File into MySQL should be one by one
            # (忽略此问题)即使表不存在，新建表也不能使用direct方式，因为不能保证此文件中的数据没有duplicate key
            return self._loadfile2db_one_by_one(fname, tablename, fields_terminated)
        # Load File into MySQL by DirectWrite LoadData
        return self._loadfile2db_direct(fname, tablename, fields_terminated, load_mode)


    def _loadfile2db_one_by_one(self, file, tablename, fields_terminated):
        db = self.conn(readonly = False, tablename = tablename)
        cur = db.cursor()
        insert = 0
        update = 0
        err = 0
        rec = 0
        f = open(file,'rb')
        while True:
            try: l = f.next()
            except StopIteration: break
            rec += 1
            values = l[:-1].split(fields_terminated)
            try:
                r = self.query_append(cur, tablename, values)
                if r == 'insert':
                    insert += 1
                elif r == 'update':
                    update += 1
            except:
                #print 'append error:',traceback.format_exc()
                log('append error. %s' %traceback.format_exc(), 1) 
                err += 1
            
        f.close()
        cur.close()
        db.close()
        return {'Records': rec,  'Added':insert ,  'Updated':update,  'Errors': err} 
            
            
    def _loadfile2db_direct(self, fname, tablename, fields_terminated, load_mode):
        from warnings import filterwarnings
        filterwarnings('ignore', category = Warning)
        if load_mode == 'append':
            load_mode = ''
        loadsql = "load data local infile '%s' %s into table %s fields terminated by '%s';" %(fname, load_mode, tablename, fields_terminated)
        #loadsql='load data local infile "'+fname+'" '+load_mode+' into table '+tablename+'  fields terminated by "'+fields_terminated+'";'
        r = self.query(loadsql,iscache=False, tablename = tablename)
        return r

    def latest_timeline(self):
        sql0 = 'select max('+self.timefield+'), \
        date_format(date_add(max('+self.timefield+'),interval -1 day),"%Y%m%d%H%i") \
        from '
        tablename = self.gettablename()
        sql = sql0 + tablename
        try:    
            return self.query(sql,iscache=False, tablename = tablename)
        except:
            yestd = time.strftime('%Y%m%d%H%M',time.localtime(time.time()-60*60*24))
            tablename = self.gettablename(yestd)
            sql = sql0 + tablename
            return self.query(sql,iscache=False, tablename = tablename)


    def rawget(self,q_dict,step_minute=None): #query_dict  #step(time): 5 (unit:minute)
        if not step_minute:
            if not q_dict.has_key('_tstep'):
                step_minute=5
            else:
                step_minute=q_dict['_tstep']
        step_sec = 60*int(step_minute)
        time1=time.time()

        #更改self.time_field字段类型为datetime
        if q_dict.has_key('_linechart'):
            time_field = 'floor((unix_timestamp(%s)+28800)/%s)*%s-28800'  %(self.timefield, step_sec, step_sec)
        else:
            time_field = 'from_unixtime(floor((unix_timestamp(%s)+28800)/%s)*%s-28800, "%s")'  %(self.timefield, step_sec, step_sec, "%Y/%m/%d %H:%i")

        #the timeline field datatype is char
        #time_field = 'from_unixtime(floor((unix_timestamp(concat(%s,"00"))+28800)/%s)*%s-28800, "%s")'  %(self.timefield, step_sec, step_sec, "%Y/%m/%d %H:%i")

        # +-28800 主要是为了去除unixtime不是从0点开始（从1970-01-01 08:00:00开始）的影响

        querylist=[]
        display_fields = []
        datatype_fields = []
        sql_s = 'select '  #select sql
        if str(step_minute) != '0':
            sql_s=sql_s+time_field+', '
            display_fields.append('时间')
            datatype_fields.append('DATETIME')
        if q_dict.get('_pageby',''):
            for j in q_dict['_pageby']:
                display_fields.append(self.fields_dispname[j])
                datatype_fields.append('STRING')
                if self.fields_alias_d.has_key(j):
                    j = self.fields_alias_d[j][0]  #(expression, comment)
                sql_s = sql_s + j + ','
        for i in q_dict['_fields']:
            display_fields.append(self.fields_dispname[i])
            datatype_fields.append('INT')
            if self.fields_alias_d.has_key(i):
                i = self.fields_alias_d[i][0]  #(expression, comment)
                i = i.replace('_tstep',str(step_minute))

                #check tstep:如果tstep=0，只允许sum,count(加)，暂不支持减乘除,avg,max,min等
                if str(step_minute) == '0':
                    for x in ('/', '*', '-', 'avg', 'max', 'min'):
                        if i.find(x) >= 0:
                            raise UnsupportedQuery('Field `%s` is not supported with `_tstep=0`' %i)
            sql_s=sql_s+i+','
        sql_s = sql_s[:-1]+' from '
        sql_w = ' where '   #where sql
        if not q_dict.has_key('_s'):
            stime=time.strftime('%Y%m%d0000')
            etime=time.strftime('%Y%m%d%H%M')
        else:
            stime=q_dict['_s']
            if not q_dict.has_key('_e'):
                etime=time.strftime('%Y%m%d%H%M')
            else:
                etime=q_dict['_e']
        try:
            querylist=self.generate_querylist(stime,etime)
        except ValueError:
            raise URLParameterError('Time Format Error  `start`:'+stime+' `end`:'+etime)

        for i in q_dict['_filters']:
            sql_w = sql_w+i+' and '

        if str(step_minute) != '0':
            if q_dict.get('_pageby',''):
                sql_g = ' group by '+time_field
                for j in q_dict['_pageby']:
                    if self.fields_alias_d.has_key(j):
                        j = self.fields_alias_d[i][0]
                    #if j in self.fields_alias.keys():
                    #    j = self.fields_alias[j][0]
                    sql_g = sql_g + ',' + j
            else:
                sql_g = ' group by '+time_field
        else:
            if q_dict.get('_pageby',''):
                sql_g = ' group by '
                for j in q_dict['_pageby']:
                    if self.fields_alias_d.has_key(j):
                        j = self.fields_alias_d[i][0]
                    #if j in self.fields_alias.keys():
                    #    j = self.fields_alias[j][0]
                    sql_g = sql_g + j + ','
                sql_g = sql_g[:-1]
            else:
                sql_g = ''

        mythread = []
        debugout = []
        for (tabname, starttime, endtime) in querylist:
            sql = '%s %s %s %s >= %s and %s <= %s %s ' %(sql_s, tabname, sql_w, \
                                            self.timefield, starttime, self.timefield, endtime, sql_g)
            args = (sql, 
                    step_minute, 
                    q_dict.get('_pageby',''),
                    q_dict.has_key('_refresh'),
                    tabname)
            t=Thread(target=self.multithreadquery, args=args)
            mythread.append(t)
            debugout.append( dict(zip(('sql','_tstep','_pageby','_refresh','tablename'), args) ))

        if q_dict.has_key('_debug'):
            raise DebugPrint(debugout)
            
        for i in mythread:
            i.start()
        for i in mythread:
            i.join()

        data= list(self.multithreadresult)
        data.sort()
        rst_cnt = len(data)
        querys = len(querylist)
        elapsed = round(time.time()-time1,3)

        log('[Data.rawget] Elapsed:%s QuerysALL:%s FromMem/Disk/NotFound:%s/%s/%s \
ProcessedRowsDisk:%s ProcessedRowsMem:%s ResultCount:%s %s' %(elapsed, querys, self.querys_frommem, self.querys_fromdisk, self.querys_notfound, self.rowsfromdisk, self.rowsfrommem, rst_cnt, q_dict))
        rst = {'Data':data, 'DataType':datatype_fields, 'Metadata':display_fields, 'Elapsed':str(elapsed)+'s', 'ProcessedRowsDisk':str(self.rowsfromdisk)+' rows', 'ProcessedRowsMem':str(self.rowsfrommem)+' rows', 'ResultCount':str(rst_cnt)+' rows'}
        self.multithreadresult=()
        self.rowsfromdisk = 0
        self.rowsfrommem = 0
        self.querys_notfound = 0
        self.querys_frommem = 0
        self.querys_fromdisk = 0
        return rst
  
    def limit_rows_get(self, data, q_dict):
        limit_rows =  q_dict.get('_limit_rows','') # limit_rows N;select_field_for_group;select_field_for_order
        pagebys = q_dict.get('_pageby')
        fields = q_dict.get('_fields')
        try:
            (num, select_field_for_group, select_field_for_order) = limit_rows.split(';')
        except:
            raise URLParameterError('The format of `_limit_rows` is: Num;select_field_for_group;select_field_for_order')
        
        # Num:前几个，正数是从小取，负数是从大取。
        try:num = int(num)
        except: raise URLParameterError('`_limit_rows`  %s is not INT' %num)

        if num > 0:
            get_v_idx = (None, num)
        elif num < 0 :
            get_v_idx = (num, None)
        else:
            raise URLParameterError('`_limit_rows`  %s cannot be zero' %num)


        # 构造分组的key
        # 用于分组的字段，如果不指定，默认按照ptime，如果指定就按照指定的字段进行分组。
        idx_key = {}
        if not select_field_for_group:
            idx_key = {0:(None,None)}
        else:
            keys = [i for i in select_field_for_group.split(',') if i]
            # 分别查找这些key的index
            for k in keys:
                if k.lower() == 'ptime':
                    idx_key[0] = (None,None)
                else:
                    try:
                        idx_key[pagebys.index(k) + 1] = (None, None)
                    except ValueError:
                        raise URLParameterError('`_limit_rows`  %s is not a pageby(groupby) field' %k)
            
        # 获取排序字段的index
        # select_field_for_order：用于排序的字段，如果只有一个select_field，则可以不指定，否则需要明确指明哪一个字段排序。
        if not select_field_for_order:
            idx_v = -1
        else:
            try:idx_v = fields.index(select_field_for_order) - len(fields)
            except ValueError: raise URLParameterError('`_limit_rows`  %s is not a select field' %select_field_for_order)


        # Process Filter 
        return FilterGroupLimit(data, idx_key, idx_v, get_v_idx)

    def get(self,q_dict,step_minute=None): #query_dict  #step(time): 5 (unit:minute)
        rst = self.rawget(q_dict,step_minute)
        data = rst['Data']
        if not data:
            return rst
        if not q_dict.get('_pageby',''):
            return rst

        time1 = time.time()
        # 如果没有limit_rows参数，并且满足如下任一条件，则直接返回rawdata
        if (not q_dict.get('_limit_rows','')) and (len(q_dict['_pageby'])>1 
                    or len(q_dict['_fields'])>1 
                    or str(q_dict.get('_tstep','')) == '0' 
                    or q_dict.has_key('_rawdata')
                 ):
            return rst

        # 如果有limit_rows参数，则直接按规则过滤
        elif q_dict.get('_limit_rows',''):
            new_data = self.limit_rows_get(data, q_dict)
            action = 'Filter'

        # 如果没有limit_rows参数，同时又都不满足上面几个条件，则会做数据填充和数据清洗
        else:
            filled = FillGroupbyWithTime(data)
            new_data = FilterNoise(filled, q_dict.get('_dataclean',3))
            action = 'Fill&Clean'

        rst['Data'] = new_data
        elapsed = round(time.time() - time1)
        new_count = len(new_data)
        log('[Data.%s] Elapsed: %s ResultCountChange %s --> %s' %(action, elapsed, rst['ResultCount'], new_count))

        elapsed += float(rst['Elapsed'][:-1])
        rst['Elapsed'] = str(elapsed)+'s'
        rst['ResultCount'] = str(new_count)+' rows'
        return rst


    def generate_querylist(self,start,end): #return tablename,starttime,endtime
        querylist=[]
        split_idx = self.table_split_idx
        start = start.ljust(14, '0')
        end = end.ljust(14, '0')
        if start[:split_idx] == end[:split_idx]:
            return [(self.gettablename(start),"'"+start+"'","'"+end+"'")]
        if split_idx == 4:
            seq = rrule.YEARLY
            postfix_s = '0101000000'
            postfix_e = '1231235959'
            fmt = '%Y'
        elif split_idx == 6:
            seq = rrule.MONTHLY
            postfix_s = '01000000'
            postfix_e = '31235959'
            fmt = '%Y%m'
        elif split_idx == 8:
            seq = rrule.DAILY
            postfix_s = '000000'
            postfix_e = '235959'
            fmt = '%Y%m%d'
        elif split_idx == 10:
            seq = rrule.HOURLY
            postfix_s = '0000'
            postfix_e = '5959'
            fmt = '%Y%m%d%H'
 
        _start = start[:split_idx]
        _end = end[:split_idx]
        for i in list(rrule.rrule(seq, dtstart=parse(_start+postfix_s),until=parse(_end+postfix_s))):
            v = i.strftime(fmt)
            if _start == v:
                querylist.append((self.gettablename(v), "'"+start+"'", "'"+v+postfix_e+"'"))
            elif _end == v:
                querylist.append((self.gettablename(v), "'"+v+postfix_s+"'", "'"+end+"'"))
            else:
                querylist.append((self.gettablename(v), "'"+v+postfix_s+"'", "'"+v+postfix_e+"'"))
        #print 'ql:',querylist
        return querylist
            

