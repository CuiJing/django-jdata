#!/usr/bin/env python
#_*_coding:utf-8 _*_

__filename__ = 'pyjdata.py'
__author__ = 'CuiJing'
__email__ = 'cuijing@jcrdb.com'
__created__ = '2012-12-18 09:53'
__description__ = ''


JDATA_URL='http://127.0.0.1:8000/jdata/'


class JdataServiceError(Exception):
    pass

class JdataRequestError(Exception):
    pass

import json
import base64,urllib2

class Jdata(object):
    def __init__(self, jdata_url = JDATA_URL):
        self.jdata_url = jdata_url
        
    def query(self, sql):
        base64_sql = base64.b64encode(sql)
        url = '%sdata/get?_sql=%s' %(self.jdata_url, base64_sql)
        rst = self._request(url)
        print '\t'.join(rst['Metadata'])
        print '-'*30
        for i in rst['Data']:
            print '\t'.join(i)
        print '-'*30
        print 'ResultCount\t',rst['ResultCount']
        print 'Elapsed\t',rst['Elapsed']
        print 'ProcessedRowsDisk\t',rst['ProcessedRowsDisk']
        print 'ProcessedRowsMem\t',rst['ProcessedRowsMem']

            

    def desc(self, objectname):
        url = '%sapi/getsettings?_o=%s' %(self.jdata_url, objectname)
        settings = self._request(url)
        meta = settings['FIELDS']
        fields_alias = settings['FIELDS_ALIAS']

        print 'MetaData:'
        print '-'*30
        for m in meta:
            print '\t',m['field'].ljust(23), m['datatype'].ljust(20), m['comment']
        print 'FieldsAlias:'
        print '-'*30
        for f in fields_alias:
            print '\t', f['field_alias'].ljust(23), f['expression'].ljust(40), f['comment']
        #for f,v in fields_alias.items():
        #    print '\t', f.ljust(30), v[0].ljust(60), v[1]

    def _request(self, url):
        content = urllib2.urlopen(url).read()
        try:
            rst = json.loads(content)
        except NameError:
            raise JdataServiceError( '`%s ... %s`' %(content[:30], content[-30:]))

        if type(rst) != dict:
            return rst
        if rst.has_key('code'):
            if rst['code'] in (1000, ):
                return rst['error']
            else:
                raise JdataRequestError(rst['error'])
        else:
            return rst
        
        

if __name__ == '__main__':
    j  = Jdata(jdata_url = 'http://10.10.83.6:87/')
    sql = "select _refresh bw from vidc_bw where ptime>201301101634 and vidc_id = '' and disp = ''"
    sql = "select _debug percent from count_idc_deli_stat where ptime >= 201301290000 and ptime <= 201301300500"
    print j.query(sql)
    #print j.query('select _debug _tstep:5 _refresh bw from cdnbw where ptime>=201208311640 and ptime<=201208311700 and cdn_type="qihu360"')
    #j.desc('mysqlstat')
    #print j.query('select _tstep:0 com_select from mysqlstat where ptime>201212240000 and cluster = "[bjct.syscdn.r]"')
    #print j.query('select com_select from mysqlstat where cluster like "[bjct.syscdn.%]" group by cluster')
    #sql = """select com_select from mysqlstat where ptime>=201212100757 and cluster in ('[bjcnc.vrs3.w]','[bjcnc.vrs3.r]','[bjcnc.ivrs3.r]','[wuhanct.ivrs3.r]','[bjct.ivrs3.r]','[bj.ivrs.r]') group by cluster"""
    #sql = """select com_update,com_delete from mysqlstat where ptime>201212260000 and cluster in ('[bjct.jddiaodulog.w]','[bjct.jddiaodulog.r]') group by cluster"""
    #sql = """select com_delete from mysqlstat where ptime>201212260000 and cluster in ('[bjct.jddiaodulog.w]','[bjct.jddiaodulog.r]') group by cluster"""
