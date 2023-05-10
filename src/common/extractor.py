import re
from datetime import datetime

from newspaper import urls, Article
from newspaper.extractors import ContentExtractor
from dateutil.parser import parse as date_parser
from pytz import utc, timezone


class CustomExtractor(ContentExtractor):

    def get_publishing_date(self, url, doc):
        """3 strategies for publishing date extraction. The strategies
        are descending in accuracy and the next strategy is only
        attempted if a preferred one fails.

        1. Pubdate from URL
        2. Pubdate from metadata
        3. Raw regex searches in the HTML + added heuristics
        """

        def parse_date_str(date_str):
            if date_str:
                try:
                    return date_parser(date_str)
                except (ValueError, OverflowError, AttributeError, TypeError):
                    # near all parse failures are due to URL dates without a day
                    # specifier, e.g. /2014/04/
                    return None

        date_match = re.search(urls.STRICT_DATE_REGEX, url)
        if date_match:
            date_str = date_match.group(0)
            datetime_obj = parse_date_str(date_str)
            if datetime_obj:
                return datetime_obj

        PUBLISH_DATE_TAGS = [
            {'attribute': 'property', 'value': 'rnews:datePublished',
             'content': 'content'},
            {'attribute': 'property', 'value': 'article:published_time',
             'content': 'content'},
            {'attribute': 'name', 'value': 'OriginalPublicationDate',
             'content': 'content'},
            {'attribute': 'itemprop', 'value': 'datePublished',
             'content': 'datetime'},
            {'attribute': 'property', 'value': 'og:published_time',
             'content': 'content'},
            {'attribute': 'name', 'value': 'article_date_original',
             'content': 'content'},
            {'attribute': 'name', 'value': 'publication_date',
             'content': 'content'},
            {'attribute': 'name', 'value': 'sailthru.date',
             'content': 'content'},
            {'attribute': 'name', 'value': 'PublishDate',
             'content': 'content'},
            {'attribute': 'pubdate', 'value': 'pubdate',
             'content': 'datetime'},
            {'attribute': 'name', 'value': 'publish_date',
             'content': 'content'},
            {'attribute': 'itemprop', 'value': 'datePublished',
             'content': 'content'},
        ]
        for known_meta_tag in PUBLISH_DATE_TAGS:
            meta_tags = self.parser.getElementsByTag(
                doc,
                attr=known_meta_tag['attribute'],
                value=known_meta_tag['value'])
            if meta_tags:
                date_str = self.parser.getAttribute(
                    meta_tags[0],
                    known_meta_tag['content'])
                datetime_obj = parse_date_str(date_str)
                if datetime_obj:
                    return datetime_obj

                try:
                    return utc.localize(datetime.fromtimestamp(int(date_str)))
                except Exception:
                    pass

        return None
