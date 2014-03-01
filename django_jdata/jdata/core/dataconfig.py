#!/usr/bin/python
#_*_coding:utf-8 _*_
#****************************************************************#
# ScriptName: dataconfig.py
# Author: CuiJing(mrcuijing@gmail.com)
# Create Date: 2014-03-01 16:32
#***************************************************************#


from django_jdata.models import DataObject, TableFields, FieldsAlias


def get_jdata_config(objectname):
    obj = DataObject.objects.get(oname = objectname)
    obj_config = {
                'objectname'  : obj.oname,
                'NAME'        : obj.oname_cn,
                'PK'          : [i.field_name for i in TableFields.objects.filter(dataobject = obj, is_key = 'PRI').order_by('pk')] ,
                'METADB'      : [{'field'   : i.field_name,
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

                'ALLOWIPS'    : obj.allowips,
                }

    return obj_config



print get_jdata_config('abc')
