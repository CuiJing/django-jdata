#_*_coding:utf-8 _*_
#!/usr/bin/python
from django import forms
from django.db import connections, DatabaseError

from .models import DataObject
from warnings import filterwarnings           
filterwarnings('ignore', category = Warning)  

class DataObjectForm(forms.ModelForm):
    def clean_table_create_sql(self):
        if not self.is_valid():
            return
        oname = self.cleaned_data['oname']
        csql = self.cleaned_data['table_create_sql']

        oname_from_sql = csql[:csql.find('(')].strip().split(' ')[-1].replace('`','')
        if oname_from_sql != oname:
            raise forms.ValidationError(u'表名(%s)跟数据名(%s)不符!' %(oname_from_sql, oname))
        
        drop_sql = 'drop table if exists %s' %oname 
        cur = connections['jdata_temp'].cursor()   
        cur.execute(drop_sql)
        try:
            cur.execute(csql)
        except DatabaseError as e:
            raise forms.ValidationError(str(e))
        return csql


    class Meta:
        model = DataObject


