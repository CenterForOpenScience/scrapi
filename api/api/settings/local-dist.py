SECRET_KEY = 'My Secret Key'

DEBUG = True

DOMAIN = 'http://localhost:8000'

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': 'scrapi',
        # 'USER': 'name',
        # 'PASSWORD': 'password',
        # 'HOST': '127.0.0.1',
        # 'PORT': '5432'
    }
}

STATIC_URL = '{}/static/'.format(DOMAIN)
