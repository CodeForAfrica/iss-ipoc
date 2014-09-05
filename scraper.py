from pprint import pprint
from urlparse import urljoin

from lxml import html
from docstash import Stash
from scrapekit import Scraper
from scrapekit.util import collapse_whitespace as clean

URL = 'http://www.ipocafrica.org/index.php?option=com_coi&view=coidisclosuredocument&id=%s&Itemid=105'

scraper = Scraper('iss-ipoc')
collection = Stash().get(scraper.name)


@scraper.task
def gen_urls():
    for i in xrange(0, 20000):
        url = URL % i
        grab_entry.queue(url)


@scraper.task
def grab_entry(url):
    doc = scraper.get(url).html()
    art = doc.find('.//div[@class="article"]')
    meta = {
        'html': html.tostring(art),
        'info_url': url
    }
    meta['person'] = art.findtext('./h3/span')
    
    for item in art.findall('.//li'):
        if 'download' in item.get('class', ''):
            doc_url = item.find('.//a').get('href')
            meta['source_url'] = urljoin(url, doc_url)
            continue

        label = item.findtext('./label')
        if label is not None:
            label = label.strip().lower().replace(' ', '_')

        content = item.find('./span')
        if content is None:
            continue

        content = html.tostring(content).split('>', 1)[-1].rsplit('<', 1)[0]
        if 'gifts' in item.get('class', ''):
            items = map(clean, content.split('<br>'))
            meta[label] = filter(lambda s: len(s), items)
        else:
            meta[label] = clean(content)
    if 'source_url' in meta:
        print meta['source_url']
        collection.ingest(meta.get('source_url'), **meta)


gen_urls.run()
