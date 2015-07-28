SECRET_KEY = 'My very Secret Key'

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


DOMAIN = 'http://localhost:8000'

STATIC_URL = '/static/'

YOUR_APP_KEY = ''
YOUR_APP_SECRET = ''
USER_OAUTH_TOKEN = ''
USER_OAUTH_TOKEN_SECRET = ''
