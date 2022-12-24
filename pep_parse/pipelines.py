import csv
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem
from pathlib import Path

from .settings import NOW_TIME


BASE_DIR = Path(__file__).parent.parent



class PepParsePipeline:
    def open_spider(self, spider):
        self.__status_dict = {}

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        if adapter.get('status'):
            pep_status = adapter['status']
            self.__status_dict[pep_status] = (
                self.__status_dict.get(pep_status, 0) + 1
            )
            return item
        else:
            raise DropItem(f'Отсутствует статус в {item}')

    def close_spider(self, spider):
        RESULT_DIR = BASE_DIR / 'results'
        filename = "status_summary_" + NOW_TIME + ".csv"
        with open(RESULT_DIR / filename, mode='w', encoding='utf-8') as f:
            csv.writer(
                f, dialect=csv.unix_dialect, quoting=csv.QUOTE_NONE
            ).writerows(
                (
                    ("Статус", "Колличество"),
                    *self.__status_dict.items(),
                    ("Total", sum(self.__status_dict.values()))
                )
            )
