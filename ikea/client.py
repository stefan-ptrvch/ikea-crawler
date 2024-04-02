"""
Interfacing with IKEA's website to get product lists.
"""

import requests
from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup


requests.packages.urllib3.disable_warnings()


class IKEAClient:
    """
    Fetch product URLs from API and pages.
    """

    _products_in_cat_endpoint = 'https://sik.search.blue.cdtapps.com/rs/sr/product-list-page/more-products?category={category_id}&start={start}&end={end}&store=050&zip=11000'
    _categories_endpoint = 'https://www.ikea.com/rs/sr/header-footer/menu-products.html'
    _category_tags_endpoint = 'https://web-api.ikea.com/dimma/statics/{product_id}?vars=show&market=rs&loc=sr'
    _category_tags_headers = {'x-consumer-id': 'cdd12761-fc45-49dc-89d2-91ac362a50eb'}

    def get_category_tags(self, product_id):
        """
        Fetches all categories that product with `product_id` belongs to.

        :param product_id: id of IKEA product
        :return: list of category (key) strings or None
        """

        response = requests.get(
            self._category_tags_endpoint.format(product_id=product_id),
            headers=self._category_tags_headers,
            timeout=10
        )

        if response.status_code != 200:
            print(f"Couldn't fetch categories from API for product with ID: {product_id}")
            return

        return response.json()['rangeIds']

    def get_categories(self):
        """
        Get category structure with names and IDs.
        """

        resp = requests.get(self._categories_endpoint, timeout=10)
        soup = BeautifulSoup(resp.text, features='html.parser')

        # Get name and data-tracking-label
        category_tree = {}
        for nav in soup.select('nav'):
            span = nav.find('span')
            category_tree[span.text] = {}
            category_tree[span.text]['sub_categories'] = []
            for sub_cat in nav.select('li > a'):
                sub_cat_id = sub_cat.get('data-tracking-label')
                if sub_cat_id == 'all':
                    continue
                sub_cat_name = sub_cat.text
                category_tree[span.text]['sub_categories'].append(
                    {
                        'sub_cat_name': sub_cat_name,
                        'sub_cat_id': sub_cat_id
                    }
                )

        return category_tree

    def get_product_range(self, session, category_id, start, end):
        """
        Fetch products in category in range between start and end.
        """

        resp = session.get(
            self._products_in_cat_endpoint.format(category_id=category_id,
                                                  start=start,
                                                  end=end),
            timeout=60)
        return resp.json()['moreProducts']['productWindow']

    def get_products_in_cat(self, category_id):
        """
        Returns list of products in a category given its ID.
        """

        # Set up session with retires, because sometimes this endpoint times
        # out
        session = requests.Session()
        retries = Retry(total=5, backoff_factor=0.1)
        session.mount('http://', HTTPAdapter(max_retries=retries))
        session.mount('https://', HTTPAdapter(max_retries=retries))

        # Paginate through products in category
        all_products = []
        start = 0
        end = 1000

        while len(products := self.get_product_range(session, category_id, start, end)) != 0:
            all_products.extend(products)
            start = end + 1
            end += 1000
        return all_products


class MontikeaClient:
    """
    Handles communication with montikea.com
    """

    _product_url_template = 'https://www.montikea.com/product/{product_id}'

    def __init__(self):
        self._montikea_session_ru = requests.Session()
        self._montikea_session_en = requests.Session()

        retries = Retry(total=5,
                        backoff_factor=0.1)

        self._montikea_session_ru.mount('http://',
                                        HTTPAdapter(max_retries=retries))
        self._montikea_session_ru.mount('https://',
                                        HTTPAdapter(max_retries=retries))

        self._montikea_session_en.mount('http://',
                                        HTTPAdapter(max_retries=retries))
        self._montikea_session_en.mount('https://',
                                        HTTPAdapter(max_retries=retries))

        # Get the cookie
        self._montikea_session_ru.get('https://www.montikea.com/locale/ru',
                                      verify=False,
                                      timeout=10)
        self._montikea_session_en.get('https://www.montikea.com/locale/en',
                                      verify=False,
                                      timeout=10)

    def get(self, product_id, locale='en'):
        # The product ID needs to have leading zeroes if it's not eight digits
        # long
        product_id_str = str(product_id)
        montikea_id = product_id_str.zfill(8)
        if locale == 'ru':
            return self._montikea_session_ru.get(
                self._product_url_template.format(product_id=montikea_id),
                timeout=10)
        return self._montikea_session_en.get(
            self._product_url_template.format(product_id=montikea_id),
            timeout=10)
