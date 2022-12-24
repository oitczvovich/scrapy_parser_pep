import scrapy


class PepSpider(scrapy.Spider):
    name = 'pep'
    allowed_domains = ['peps.python.org']
    start_urls = ['https://peps.python.org/']

    def parse(self, response):
        """собирает ссылки на документы PEP."""
        all_peps = response.xpath(
            '//a[@class="pep reference internal"]/@href'
        ).getall()
        for pep_link in all_peps:
            yield response.follow(pep_link, callback=self.parse_pep)

    def parse_pep(self, response):
        """парсит страницы с документами и формирует Items."""
        full_pep_name = response.css('.page-title::text').get().split()
        number = full_pep_name[1]
        name_pep = ' '.join(full_pep_name[3:])
        yield {
            'number': int(number),
            'name': name_pep,
            'status': response.css('abbr::text').get()
        }
