django-jdata
============

Jdata module for Django


## Installation
Run this command to install django-jdata

    pip install django-jdata

### Requirements
Django >= 1.4 (1.5 custom user is supported)

## Usage
Add the app to installed apps


```python
INSTALLED_APPS = (
    ...
    'django_jdata',
    ...
)
```

Add the middleware to MIDDLEWARE_CLASSES

```python
MIDDLEWARE_CLASSES = (
    ...
    'django_jdata.middleware.JdataMiddleware',
    ...
)
```




Add urls to your *urls.py*

```python
urlpatterns = patterns('',
    ...
    url(r'^jdata/', include('django_jdata.urls')),
    ...
)
```


## Settings

