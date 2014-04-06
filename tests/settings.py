"""
"""

# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
import os
BASE_DIR = os.path.dirname(os.path.dirname(__file__))


# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/1.6/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = 'b04+ve+vi0!d!k0p^7l^%=t^y9y5nr7b^v5r6t#4-asji'

# SECURITY WARNING: don't run with debug turned on in production!
#DEBUG = False
DEBUG = True

TEMPLATE_DEBUG = True

ALLOWED_HOSTS = ['127.0.0.1','localhost']


# Application definition

INSTALLED_APPS = (
    #'django_admin_bootstrapped.bootstrap3',
    'django_admin_bootstrapped',
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'django_nvd3',
    'djangobower',
    'django_jdata',
)

MIDDLEWARE_CLASSES = (
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    #'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
    'django_jdata.middleware.JdataMiddleware',
)

ROOT_URLCONF = 'tests.urls'

WSGI_APPLICATION = 'tests.wsgi.application'


# Database
# https://docs.djangoproject.com/en/1.6/ref/settings/#databases

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.mysql',
        'NAME': 'jdata',
        'USER': 'jdata',
        'PASSWORD': 'jdata',
        'HOST': '127.0.0.1'
    },
    'jdata_temp': {
        'ENGINE': 'django.db.backends.mysql',
        'NAME': 'test',
        'USER': 'jdata',
        'PASSWORD': 'jdata',
        'HOST': '127.0.0.1'
    }

}

# Internationalization
# https://docs.djangoproject.com/en/1.6/topics/i18n/

#LANGUAGE_CODE = 'en-us'
LANGUAGE_CODE = 'zh-CN'

#TIME_ZONE = 'UTC'
TIME_ZONE = 'Asia/Shanghai'

USE_I18N = True

USE_L10N = True

USE_TZ = True


# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/1.6/howto/static-files/
STATIC_URL = '/static/'

# djangobower
#STATICFILES_FINDERS = (
#                        'djangobower.finders.BowerFinder', 
#                        )
#BOWER_COMPONENTS_ROOT = BASE_DIR + '/components/'
#BOWER_PATH = '/usr/local/bin/bower'
#BOWER_INSTALLED_APPS = (
#                       'nvd3',
#                        )
### end djangobower


DATETIME_FORMAT = 'Y-m-d H:i:s'


## settings for django_jdata
JDATA_TEMPDIR = '/tmp/jdata'
print 'BASE',BASE_DIR
#TEMPLATE_DIRS = ( BASE_DIR + '/labs/templates/', )
