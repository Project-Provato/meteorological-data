# https://whatacold.io/blog/2022-04-09-scrapy-csv-without-header/

from scrapy.exporters import CsvItemExporter


class HeadlessCsvItemExporter(CsvItemExporter):
    def __init__(self, *args, **kwargs):

        # args[0] is (opened) file handler
        # if file is not empty then skip headers
        if args[0].tell() > 0:
            kwargs['include_headers_line'] = False

        super(HeadlessCsvItemExporter, self).__init__(*args, **kwargs)