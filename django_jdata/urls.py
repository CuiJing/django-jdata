from django.conf.urls import patterns, include, url

urlpatterns = patterns('django_jdata.views',
    # Examples:
    # url(r'^$', 'labs.views.home', name='home'),
    # url(r'^blog/', include('blog.urls')),

    url(r'^data/get', 'get'),
    #url(r'^web/', include("app.urls")),

)
