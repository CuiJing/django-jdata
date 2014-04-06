#!/usr/bin/python
#_*_coding:utf-8 _*_
#****************************************************************#
# ScriptName: dataconfig.py
# Author: CuiJing(mrcuijing@gmail.com)
# Create Date: 2014-03-01 16:32
#***************************************************************#


from django_jdata.models import DataObject, TableFields, FieldsAlias, MySQLService, TableLocation
from django_jdata.exceptions import ObjectNotExists, TableNotExistsInMetaDB


def get_jdata_config(objectname):
    try:
        obj = DataObject.objects.get(oname = objectname)
    except:
        raise ObjectNotExists('DataObject `%s` not exists!' %objectname)
    obj_config = {
                'NAME':             obj.oname,
                'NAME_DESC':        obj.oname_cn,
                'TABLE_SPLIT_IDX':  obj.table_split_idx,
                'PK':               [i.field_name for i in TableFields.objects.filter(dataobject = obj, is_key = 'PRI').order_by('pk')] ,
                'FIELDS':           [{'field'   : i.field_name,
                                'datatype': i.field_datatype,
                                'comment' : i.field_comment,
                                } for i in TableFields.objects.filter(dataobject = obj).order_by('pk')
                                ],
                'FIELDS_ALIAS': [{
                                    'field_alias': i.field_name,
                                    'expression' : i.field_expression,
                                    'comment'    : i.field_name_cn
                                } for i in FieldsAlias.objects.filter(dataobject = obj)
                                ],

                'ALLOWIPS'    : [ i.strip() for i in obj.allowips.split(',') if i.strip()],
                'CREATE_SQL'  : obj.table_create_sql,
                }

    return obj_config

# get mysql service by tablename and objectname
def get_mynode_by_tablename(objectname, tablename, readonly):
    try:
        mysql = TableLocation.objects.get(tname = tablename, dataobject = objectname).mysqlservice
    except TableLocation.DoesNotExist:
        raise TableNotExistsInMetaDB('%s: Table `%s` not found in DMC' %(objectname, tablename))
    if readonly:
        return mysql.reader
    else:
        return mysql.writer

# get next mysql service for objectname
def get_next_mynode_roundrobin(objectname):
    try:
        last_mynode_id = TableLocation.objects.filter(dataobject = objectname).order_by('-created')[0].mysqlservice_id
    except IndexError:
        last_mynode_id = 0
    try:
        next_mynode_id = MySQLService.objects.filter(id__gt=last_mynode_id, servicemode='RW').order_by('id')[0].id
    except IndexError:
        next_mynode_id = MySQLService.objects.filter(servicemode='RW').order_by('id')[0].id
    return next_mynode_id

def get_next_mynode_load(objectname):
    next_mynode_id = MySQLService.objects.order_by('load')[0].id
    return next_mynode_id

def add_tablelocation(objectname, mysqlservice_id, tablename ):
    tl = TableLocation(dataobject=DataObject(objectname), 
                    mysqlservice = MySQLService(mysqlservice_id),
                    tname = tablename)
    tl.save()

def remove_tablelocation(objectname, tablename):
    TableLocation.objects.filter(dataobject = objectname, 
                            tname = tablename).delete()


def get_tablelocation(objectname):
    return TableLocation.objects.filter(dataobject = objectname).all()





