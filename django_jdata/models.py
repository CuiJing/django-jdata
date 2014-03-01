# -*- coding:utf-8 -*-
# Create your models here.
from django.db import models


class DataObject(models.Model):
    u"""
    """
    oname = models.CharField(u'数据名(仅字母,全局唯一)',max_length=32,primary_key=True)
    oname_cn = models.CharField(u'数据描述',max_length=100)  
    conf = models.TextField(u'数据配置',max_length=200, blank=True, null=True)
    created = models.DateTimeField('创建时间', auto_now_add=True)
    # db
    table_split_idx = models.IntegerField('分表规则', max_length=10, choices=((6,'月'),(8,'天'),(10,'小时')), default=8 )
    table_create_sql = models.TextField('建表语句',max_length=1000, default = '''create table dataobject_name (ptime datetime  comment '时间',
 key1 varchar(30) comment '字符型字段key1', 
key2 varchar(30) comment '字符型字段key2', 
v1 int  comment '数值型字段value1', 
v2 int comment '数值型字段value2',
primary key  (ptime, key1, key2) 
)
''', help_text = 'PS.请填写完整的建表语句（表名须与数据名相同）')

    #security
    allowips = models.CharField('IP白名单',max_length=200, blank=True)


    def __unicode__(self):
        return self.oname

    class Meta:
        verbose_name = '数据对象(DataObject)'
        verbose_name_plural = '1.[数据对象]'


class MySQLService(models.Model):
    mname = models.CharField('名字',max_length=100)
    writer = models.CharField('写库',max_length=100)
    reader = models.CharField('读库',max_length=100)
    weight = models.IntegerField('权重',max_length=10)
    servicemode = models.CharField('模式', max_length=10)
    created = models.DateTimeField('创建时间', auto_now_add=True)

    def __unicode__(self):
        return self.mname

    class Meta:
        verbose_name = 'MySQL服务'
        verbose_name_plural = '2.[MySQL服务]'

class TableLocation(models.Model):
    dataobject = models.ForeignKey(DataObject)
    mysqlservice = models.ForeignKey(MySQLService)
    tname = models.CharField('表名', max_length=64, unique=True)
    created = models.DateTimeField('创建时间', auto_now_add=True)

    def __unicode__(self):
        return self.tname

    class Meta:
        verbose_name = '数据表分布'
        verbose_name_plural = '3.[数据表分布]'


class FieldsAlias(models.Model):
    dataobject = models.ForeignKey(DataObject)  
    field_name = models.CharField('字段名', max_length=32, help_text = '一个用户自定义的字段名，与表中重复字段以此为准')
    field_expression = models.CharField('字段表达式', max_length=32, help_text='可以是一个或多个字段的表达式，例如: sum(ifnull(value1,0)+ifnull(value2,0))')
    field_name_cn = models.CharField('字段注释', max_length=32, help_text='便于理解的中文名字')  

    def __unicode__(self):
        return self.field_name

    class Meta:
        verbose_name = '自定义字段'
        verbose_name_plural = '自定义字段'
        unique_together = (('dataobject', 'field_name',),)
        

class TableFields(models.Model):
    dataobject = models.ForeignKey(DataObject)
    field_name = models.CharField('字段名',max_length=64)
    field_datatype = models.CharField('字段类型',max_length=32)
    is_null = models.CharField('是否为空', max_length=3, choices=(('YES','是',),('NO','否',)))
    is_key = models.CharField('主键', max_length=10, blank=True, null=True )
    field_default = models.CharField('默认值',max_length=64, blank=True, null=True)
    field_comment = models.CharField('字段注释',max_length=64, blank=True, null=True)

    def __unicode__(self):
        return self.field_name

    class Meta:
        verbose_name = '表字段'
        verbose_name_plural = '表字段'
        unique_together = (('dataobject', 'field_name'),)



