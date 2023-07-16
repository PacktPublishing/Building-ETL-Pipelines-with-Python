# Logging is an importent part of code to catch errors or exceptions. Let's introduce logging to make our code debuggable. It's a good idea to creat a logfile. A logfile can be used for many purpose including but not limited to anayalyizng any faiure or finding bugs in the code. Let's create an universal logging config that can be used in all python modules without writing the boilerplate codes.

# In[51]:


import logging
import logging.config
import os.path

def log_config():
    config = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'standard': {
                'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
            },
        },
        'handlers': {
            'default_handler': {
                'class': 'logging.FileHandler',
                'level': 'DEBUG',
                'formatter': 'standard',
                'filename': os.path.join('logs', 'etl_pipeline.log'),
                'encoding': 'utf8'
            },
        },
        'loggers': {
            '': {
                'handlers': ['default_handler'],
                'level': 'DEBUG',
                'propagate': False
            }
        }
    }
    logging.config.dictConfig(config)

if __name__ == '__main__':
    log_config()
