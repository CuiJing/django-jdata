## -*- coding:utf-8 -*-    
from django.contrib import admin
from django import forms
from django.db import connections

from .models import DataObject, MySQLService, TableLocation, FieldsAlias, TableFields
from .forms import DataObjectForm
# Register your models here.



# 用建表的SQL刷新TableFields对象，其实是取自于jdata_temp库中的表结构
def auto_add_tablefields(obj):
    cur = connections['jdata_temp'].cursor()
    sql = '''select COLUMN_NAME, COLUMN_TYPE,IS_NULLABLE,  COLUMN_KEY, COLUMN_DEFAULT, COLUMN_COMMENT  
                from information_schema.columns \
                where TABLE_SCHEMA=schema() and TABLE_NAME = '%s' order by ORDINAL_POSITION;'''
    cur.execute(sql %obj.oname)
    fields = cur.fetchall()
    for (name, datatype, is_null, is_key, filed_default, comment) in fields:
        f = TableFields(dataobject = obj,
                    field_name = name,
                    field_datatype = datatype,
                    is_null = is_null,
                    is_key = is_key,
                    field_default = filed_default,
                    field_comment = comment,
                    )
        f.save()

class FieldsAliasInline(admin.TabularInline):
    model = FieldsAlias
    extra = 1

class TableFieldsInline(admin.TabularInline):
    model = TableFields
    extra = 0
    readonly_fields = (
                    'id',
                    'field_name', 
                    'field_datatype', 
                    'is_null', 
                    'is_key',
                    'field_default',
                    'field_comment',
                    )
class TableLocationInline(admin.TabularInline):
    model = TableLocation
    extra = 0
    max_num = 3
    readonly_fields = (
                    'id',
                    'mysqlservice',
                    'tname',
                    'created',
                    )


class DataObjectAdmin(admin.ModelAdmin):
    search_fields = ('oname', 'conf',)
    list_display = (
                'oname', 
                'oname_cn',
                'table_split_idx',
                #'table_fields',
                'created',
                )

    #list_filter = ('last_slowlog_time',)
    ordering = ('-created',)
    fieldsets = (
        ('创建：', {
        'fields' : (  ('oname',
                'oname_cn',),
                'table_split_idx',
                'table_create_sql',
                ),
        #'description': '<div class="help">请填写完整的建表语句（表名须与数据名相同）</div>' ,
        }),
        (u'更多...', {
        'classes': ('collapse',),
        'fields':(
            'allowips',
            'conf',
            )
        }),
        )

    inlines = [FieldsAliasInline, TableFieldsInline, TableLocationInline,]
    form = DataObjectForm

    def save_model(self, request, obj, form, change):
        obj.save()
        # 在这里刷新字段报错，因为这个save_model函数是在inline form做validate检查之前执行的,所以才重写下面两个函数
        #TableFields.objects.filter(dataobject = obj).delete()
        #auto_add_tablefields(obj)

    def response_add(self, request, new_obj):
        auto_add_tablefields(new_obj)
        return super(DataObjectAdmin, self).response_add(request, new_obj)

    def response_change(self, request, obj):
        TableFields.objects.filter(dataobject = obj).delete()
        auto_add_tablefields(obj)
        return super(DataObjectAdmin, self).response_change(request, obj)

    # drop obj's temp table *before* obj.delete()
    def delete_model(self, request, obj):
        cur = connections['jdata_temp'].cursor()
        cur.execute('drop table if exists %s' %obj.oname)
        obj.delete()
        
admin.site.register(DataObject, DataObjectAdmin)


class MySQLServiceAdmin(admin.ModelAdmin):
    search_fields = ('mname', 'writer', 'reader',)
    list_display = (
                    'mname',
                    'writer',
                    'reader',
                    'load',
                    'weight',
                    'servicemode',
                    'created',
                    )
    fields = (
                'mname',
                'writer',
                'reader',
                'weight',
                'servicemode',
                )

admin.site.register(MySQLService, MySQLServiceAdmin)

class TableLocationAdmin(admin.ModelAdmin):
    search_fields = ('dataobject', 'tname',)
    list_display = ( 'dataobject', 
                    'mysqlservice',
                    'tname',
                    'created',
                    )
    fields = ('dataobject',
                'mysqlservice',
                'tname',
                )

#admin.site.register(TableLocation, TableLocationAdmin)                
