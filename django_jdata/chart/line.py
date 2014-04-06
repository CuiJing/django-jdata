#!/usr/bin/python
#_*_coding:utf-8 _*_
import time, datetime, random
from django.shortcuts import render_to_response

def linewithfocuschart(request):
    """
    linewithfocuschart page
    """
    nb_element = 100
    start_time = int(time.mktime(datetime.datetime(2012, 6, 1).timetuple()) * 1000)

    xdata = range(nb_element)
    xdata = map(lambda x: start_time + x * 1000000000, xdata)
    ydata = [i + random.randint(1, 10) for i in range(nb_element)]
    ydata2 = map(lambda x: x * 2, ydata)
    ydata3 = map(lambda x: x * 3, ydata)
    ydata4 = map(lambda x: x * 4, ydata)

    tooltip_date = "%Y-%m-%d %H:%M:%S"
    #tooltip_date = "%d %b %Y %H:%M:%S %p"
    extra_serie = {"tooltip": {"y_start": "There are ",
                                "y_end": " calls"},
                   "date_format": tooltip_date}
    chartdata = {
        'x': xdata,
        'name1': 'series 1', 'y1': ydata, 'extra1': extra_serie,
        'name2': 'series 2', 'y2': ydata2, 'extra2': extra_serie,
        'name3': 'series 3', 'y3': ydata3, 'extra3': extra_serie,
        'name4': 'series 4', 'y4': ydata4, 'extra4': extra_serie
    }
    charttype = "lineWithFocusChart"
    data = {
        'charttype': charttype,
        'chartdata': chartdata,
        'extradata': {
                    'x_is_date': True,
                    'x_axis_format': '%Y-%m-%d %H',
                    }
    }
    return render_to_response('linewithfocuschart.html', data)



from django_jdata.views import get

def line(request):
    '''

    their are two kinds of linecharts:
    1) pageby 1 column and query 1 field, draw lines per column values
    http://127.0.0.1:8000/jdata/chart/line?_o=mysqlstat&_s=201303280012&_e=201303282252&_fields=queries&host=10.11.53.27&_pageby=cluster
    2) no pageby, draw lines per fields
    http://127.0.0.1:8000/jdata/chart/line?_o=mysqlstat&_s=201303280012&_e=201303282252&_fields=queries,com_select&host=10.11.53.27

    '''

    #get result by `views.get` function
    data = get(request, linechart = True)
    dt = data['DataType']
    if dt[0] != 'DATETIME':
        raise ChartLineError('The first field must be DATETIME')
    if dt[-1] != 'INT':
        raise ChartLineError('The last field must be INT')
    if dt[1] == 'STRING':
        if len(dt) != 3:
            raise ChartLineError('Their must be 3 fields if second field is string')
        return _linechart_by_pageby(data)
    elif [ i for i in dt[1:] if i != 'INT']:
        raise ChartLineError('The fields after first must be INT if second field is not string')
    else:
        return _linechart_by_multi_fields(data)

def _linechart_by_pageby(data):
    rawdata = data['Data']
    timeline = {}
    lines = {}
    for (time, col, val) in rawdata:
        try:
            val = int(val)
        except:
            val = '%s' %val
        if not timeline.has_key(time):
            timeline[time] = 0
        if lines.has_key(col):
            lines[col].append(val)
        else:
            lines[col] = [val,]
    data4chart = {'xdata': timeline.keys(),
            'lines': lines,
    }
    return _render2chart(data4chart)

def _linechart_by_multi_fields(data):
    rawdata = data['Data']
    meta = data['Metadata']
    xdata = []
    lines = {}
    for i in range(len(meta) - 1):
        lines[meta[i + 1]] = [] 

    for li in rawdata:
        for i in range(len(li)):
            try:
                val = int(li[i])
            except:
                val = '%s' %li[i]
            if i == 0:
                xdata.append(val)
            else:
                lines[meta[i]].append(val)
    data4chart = {'xdata': xdata,
                'lines': lines,
    }
    return _render2chart(data4chart)
    

def _render2chart(data):
    xdata = data['xdata']
    xdata = map(lambda x: int(x) * 1000, xdata)
    xdata.sort()

    extra_serie = {"tooltip": {"y_start": "", 
                            "y_end": ""},     
               "date_format": "%Y-%m-%d %H:%M"}         

    chartdata =  {
        'x': xdata,
        }
    li_idx = 0 #line index
    for (linename, vals) in data['lines'].items():
        li_idx += 1
        name = 'name%s' %li_idx
        y = 'y%s' %li_idx
        extra = 'extra%s' %li_idx

        chartdata[name] = linename
        chartdata[y] = vals
        chartdata[extra] = extra_serie

    chart_context = {
        'charttype': 'lineChart',
        #'charttype': 'lineWithFocusChart',
        'chartdata': chartdata,
        'extradata': {
                    'x_is_date': True,
                    'x_axis_format': '%Y-%m-%d %H',
                    }
    }
    return render_to_response('linewithfocuschart.html', chart_context)


