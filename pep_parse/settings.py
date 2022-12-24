from datetime import datetime
from pathlib import Path

BOT_NAME = 'pep_parse'
SPIDER_MODULES = ['pep_parse.spiders']
NEWSPIDER_MODULE = 'pep_parse.spiders'
ROBOTSTXT_OBEY = True
ITEM_PIPELINES = {
   'pep_parse.pipelines.PepParsePipeline': 300,
}
FEEDS = {
    'results/pep_%(time)s.csv': {
        'format': 'csv',
        'fields': ['number', 'name', 'status'],
        'overwrite': True
    },
}

NOW_TIME = datetime.strftime(datetime.now(), '%Y-%m-%dT%H-%M-%S')
BASE_DIR = Path(__file__).parent.parent
