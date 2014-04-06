#_*_coding:utf-8 _*_
from django.test import TestCase,Client
from django_jdata.models import DataObject, MySQLService, FieldsAlias
from django_jdata.admin import auto_add_tablefields

class DataObjectTestCase(TestCase):
    def setUp(self):
        self.oname = 'testcase1'

        #create dataobject
        self.obj = DataObject(oname = self.oname,
                    oname_cn = 'testcase No.1',
                    table_split_idx = 8,
                    table_create_sql = "create table testcase1(\
                    timeline char(12) comment '时间',\
                    shopname varchar(32) comment '店铺名称',\
                    shoptype varchar(32) comment '店铺分类',\
                    region varchar(32) comment '区域',\
                    channel varchar(32) comment '渠道',\
                    income varchar(32) comment '收入',\
                    cost varchar(32) comment '成本',\
                    fee varchar(32) comment '费用',\
                    primary key(timeline, shoptype, region, channel))",
                    allowips = '',
                    )
        self.obj.save()

        #auto add tablefields
        auto_add_tablefields(self.obj)

        #create mysqlservices
        self.m1 = MySQLService(mname='testmysql1',
                        writer = 'jdata/jdata@127.0.0.1/jdata1',
                        reader = 'jdata/jdata@127.0.0.1/jdata1',
                        )
        self.m1.save()
        self.m2 = MySQLService(mname='testmysql2',
                        writer = 'jdata/jdata@127.0.0.1/jdata2',
                        reader = 'jdata/jdata@127.0.0.1/jdata2',
                        )
        self.m2.save()
        self.m3 = MySQLService(mname='testmysql3',
                        writer = 'jdata/jdata@127.0.0.1/jdata3',
                        reader = 'jdata/jdata@127.0.0.1/jdata3',
                        )
        self.m3.save()

        self.c = Client()

    def test_all(self):
        '''
        data = {'oname': self.oname, 
                'oname_cn':  'testcase No.1',
                'table_split_idx' : 8,
                'table_create_sql' : "create table testcase1(\
                    timeline char(12) comment '时间',\
                    shopname varchar(32) comment '店铺名称',\
                    shoptype varchar(32) comment '店铺分类',\
                    region varchar(32) comment '区域',\
                    channel varchar(32) comment '渠道',\
                    income varchar(32) comment '收入',\
                    cost varchar(32) comment '成本',\
                    fee varchar(32) comment '费用',\
                    primary key(timeline, shoptype, region, channel))",
                'allowips': '',
                }
        res = self.c.post('/admin/django_jdata/dataobject/add/', data)
        self.assertEqual(res.status_code, 200)
        '''
        self.assertEqual(MySQLService.objects.count(), 3)
        m = MySQLService.objects.all()[0]
        self.assertEqual(m.load , 1)
        self.assertEqual(m.weight, 100)
        self.assertEqual(m.servicemode, 'RW')
        
        d = DataObject.objects.get(oname = self.oname)
        self.assertEqual(str(d), self.oname)
        print self.obj.__dict__

        response = self.c.get('/jdata/api/setting', data={'_o':self.oname})
        self.assertEqual(response.status_code ,200)

