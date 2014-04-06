from django.conf.urls import patterns, include, url


urlpatterns = patterns('django_jdata.views',
    # Examples:
    # url(r'^$', 'labs.views.home', name='home'),
    # url(r'^blog/', include('blog.urls')),

    url(r'^data/get', 'get'),
    url(r'^data/put', 'put'),
    url(r'^api/getsettings', 'getsettings'),
    url(r'^api/getquerydict', 'getquerydict'),
    url(r'^api/gettables', 'gettables' ),
    url(r'^api/getlatesttime', 'getlatesttime' ),
    url(r'^api/droptables', 'droptables' ),
) + patterns('django_jdata.chart',
    url(r'^chart/line', 'line.line', ),
    )
