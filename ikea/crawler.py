import concurrent.futures as futures
from datetime import datetime
import requests
import math
from tqdm import tqdm
from bs4 import BeautifulSoup
from .client import IKEAClient


class IKEACrawler:
    """
    Gets all products off of IKEA's website (HOPEFULLY).
    """

    _ikea_client = IKEAClient()

    def __init__(self, num_products=None):
        self.products = []
        self._crawled_ids = []
        self._num_products = num_products
        self._done = False

    def _process_single_product(self, prod, category, sub_category):
        """
        Parses product page for data that's not available on the category page.
        """

        # First check whether we already crawled this product
        numeric_filter = filter(str.isdigit, prod['id'])
        product_id = int(''.join(numeric_filter))
        if product_id in self._crawled_ids:
            return
        self._crawled_ids.append(product_id)

        product_name = prod['name']
        product_description = prod['mainImageAlt']
        product_description = product_description.replace(f'{product_name} ', '')

        main_image_url = prod['mainImageUrl']
        product_url_ikea = prod['pipUrl']
        price_rs = prod['salesPrice']['numeral']
        price = math.ceil(prod['salesPrice']['numeral']*1.44/117)

        # Get price from Croatia
        try:
            hr_product_url = product_url_ikea[:20] + '/hr/hr/' + product_url_ikea[27:]
            resp = requests.get(hr_product_url, timeout=10)
            soup = BeautifulSoup(resp.text, features='html.parser')
            price_div = soup.find('div', class_='pip-temp-price-module__price')
            if price_div:
                try:
                    integer_part = int(price_div.find('span', {'class': 'pip-temp-price__integer'}).text.replace('.', ''))
                except Exception:
                    integer_part = 0
                try:
                    decimal_part = int(price_div.find('span', {'class': 'pip-temp-price__decimal'}).text[1:])
                except Exception:
                    decimal_part = 0
                price_hr = integer_part + decimal_part/100
            else:
                raise ValueError(f"Price div element missing for product {hr_product_url}")
        except Exception as e:
            print(f"Error while fetching HR price, {type(e).__name__}: {e}")
            price_hr = None

        availability = False
        for store in prod['availability']:
            if 'store' not in store:
                continue
            if store['store'] != 'Beograd':
                continue
            availability = not store['status'] == 'OUT_OF_STOCK'

        # Get the product off of IKEA's website since the JSON isn't enough
        try:
            resp = requests.get(product_url_ikea, timeout=10)
        except Exception:
            return
        soup = BeautifulSoup(resp.text, features='html.parser')

        # Get long product description
        div = soup.find('div', {'class': 'pip-product-details__container'})
        ps = div.find_all('p', {'class': 'pip-product-details__paragraph'})
        long_desc = [p.get_text() for p in ps]
        long_desc = '\n\n'.join(long_desc)

        # Get materials
        span = soup.find('span', {'class': 'pip-product-details__material-header'})
        try:
            dls = span.findNext('div').find_all('dl')
            materials = []
            for dl in dls:
                dt = dl.find('dt')
                dd = dl.find('dd')
                if dt:
                    materials.append(f'{dt.get_text()} {dd.get_text()}')
                else:
                    materials.append(dd.get_text())
            materials = '\n'.join(materials)
        except Exception:
            materials = ''

        # Get product images
        divs = soup.find_all('div', {'class': 'pip-media-grid__grid'})
        imgs = divs[0].find_all('img')
        other_image_urls = [img['src'] for img in imgs]

        # Get packaging dimensions and weight
        divs = soup.find_all('div', {'class': 'pip-product-dimensions__package-container'})

        volume_total = 0
        weight_total = 0
        max_dim = 0
        num_packages = 0
        product_parts = []

        for div in divs:
            # Get product ID
            span = div.find('span', {'class': 'pip-product-identifier'}).find('span', {'class': 'pip-product-identifier__value'})
            composite_product_id = int(span.text.replace('.', '').lstrip('0'))
            product_parts.append(composite_product_id)

            # Get dimensions, num packages and weight
            measurements_div = div.find('div', {'class': 'pip-product-dimensions__measurement-container'})
            width = None
            height = None
            length = None
            weight = None
            measurements_ps = measurements_div.find_all('p')
            for measurements_p in measurements_ps:
                text = measurements_p.text
                if 'Širina' in text:
                    width = int(measurements_p.contents[1].split(' ')[0])/100
                    if width > max_dim:
                        max_dim = width
                elif 'Visina' in text:
                    height = int(measurements_p.contents[1].split(' ')[0])/100
                    if height > max_dim:
                        max_dim = height
                elif 'Dužina' in text:
                    length = int(measurements_p.contents[1].split(' ')[0])/100
                    if length > max_dim:
                        max_dim = length
                elif 'Težina' in text:
                    weight = float(measurements_p.contents[1].split(' ')[0])
                elif 'Pakovanje' in text:
                    num_packages += int(measurements_p.contents[1].text)

            if width and height and length:
                volume_total += width*height*length

            if weight:
                weight_total += weight

        num_of_packages = num_packages if num_packages else None
        sum_volume = round(volume_total, 2) if volume_total else None
        sum_weight = round(weight_total, 2) if weight_total else None
        max_dimension = max_dim if max_dim else None

        # Check if the product is multi-pack or not
        multi_pack = soup.find('div', {'class': 'pip-product-dimensions__multi-pack'})

        breadcrumb = [cat['key'] for cat in prod['categoryPath']]
        if not (category_tags := self._ikea_client.get_category_tags(product_id)):
            category_tags = breadcrumb

        modified_date = datetime.now().strftime('%H-%M-%S %d-%m-%Y')
        product = {
            'product_name': product_name,
            'product_description': product_description,
            'product_long_description': long_desc,
            'product_id': product_id,
            'main_image_url': main_image_url,
            'other_image_urls': other_image_urls,
            'product_url_ikea': product_url_ikea,
            'price': price,
            'price_rs': price_rs,
            'price_hr': price_hr,
            'availability': availability,
            'num_of_packages': num_of_packages,
            'multi_pack': bool(multi_pack),
            'product_parts': product_parts,
            'sum_volume': sum_volume,
            'sum_weight': sum_weight,
            'materials': materials,
            'max_dimension': max_dimension,
            'modified_date': modified_date,
            'breadcrumb_categories': breadcrumb,
            'category_tags': category_tags
        }

        return product

    def _process_products_concurrent(self, products, category, sub_category):
        """
        Gets all required data from a list of products. Uses multi-threading.
        """

        # Go through each product and get data
        processed_products = []
        with futures.ThreadPoolExecutor(max_workers=100) as executor:
            future_to_url = (executor.submit(self._process_single_product,
                                             prod,
                                             category,
                                             sub_category) for
                             prod in products)

            prod_pbar = tqdm(total=len(products), leave=False)
            prod_pbar.set_description(f'Products in {sub_category}')

            for future in futures.as_completed(future_to_url):
                try:
                    product = future.result()
                    if not product:
                        continue
                    processed_products.append(product)
                    prod_pbar.update(len(processed_products) - prod_pbar.n)

                except Exception as exc:
                    print('Something went wrong!')
                    print(exc)

                # Check whether we've reached a product limit, if there's any
                if self._num_products and len(self.products) + len(processed_products) == self._num_products:
                    self._done = True
                    executor.shutdown(wait=False, cancel_futures=True)
                    break

        return processed_products

    def _process_products(self, products, category, sub_category):
        """
        Gets all required data from a list of products.
        """

        # Go through each product and get data
        processed_products = []
        prod_pbar = tqdm(total=len(products), leave=False)
        prod_pbar.set_description(f'Products in {sub_category}')
        for prod in products:
            try:
                product = self._process_single_product(prod,
                                                       category,
                                                       sub_category)
            except Exception as e:
                print(f"There was an error: {e}")
                continue

            if not product:
                continue
            processed_products.append(product)
            prod_pbar.update(len(processed_products) - prod_pbar.n)

            # Check whether we've reached a product limit, if there's any
            if self._num_products and len(self.products) + len(processed_products) == self._num_products:
                self._done = True
                break

        return processed_products

    def run(self, concurrent=False):
        """
        Returns all articles from the website.
        """

        # Get category structure
        category_tree = self._ikea_client.get_categories()

        # Go through categories and subcategories and get each product
        for cat in (cat_pbar := tqdm(category_tree)):
            cat_pbar.set_description(f"Category {cat}")

            for sub_cat in category_tree[cat]['sub_categories']:
                # This subcat doesn't have any products, so we skip it
                if sub_cat['sub_cat_name'] == 'Restoran i Bistro':
                    continue

                try:
                    products = self._ikea_client.get_products_in_cat(
                        sub_cat['sub_cat_id'])
                except Exception as e:
                    continue

                if concurrent:
                    self.products.extend(self._process_products_concurrent(
                        products, cat, sub_cat['sub_cat_name']))
                else:
                    self.products.extend(self._process_products(
                        products, cat, sub_cat['sub_cat_name']))

                if self._done:
                    break
            if self._done:
                break
