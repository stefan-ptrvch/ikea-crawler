"""
Item processing pipeline.
"""

import pandas as pd
from tqdm import tqdm
from bs4 import BeautifulSoup
from ikea.client import MontikeaClient
from ikea.storage import StorageService


class Pipeline:
    def __init__(self):
        self._montikea_client = MontikeaClient()
        self._process_report = {}

    def save_items(self, items):
        storage_service = StorageService()
        storage_service.upsert(items)

    def process_items(self, items):
        """
        Populate additional fields, data formatting and reporting.
        """

        # Drop duplicates
        df = pd.DataFrame(items)
        df = df.drop_duplicates(subset=['product_id'])
        items = df.to_dict(orient='records')

        # Separate into existing and new items
        storage_service = StorageService()
        try:
            existing_items, new_items = storage_service.get_diff(items)
        except Exception as e:
            message = f"Error while processing items {type(e).__name__}: {e}"
            print(message)
            existing_items = items
            new_items = []

        # Only add translations to new items, since we have limited API calls
        for item in tqdm(new_items, desc="Translating items"):
            self._translate_description_fields(item)

        items = []
        items.extend(existing_items)
        items.extend(new_items)

        # Convert columns with lists to string representation
        df = pd.DataFrame(items)
        for key in ('other_image_urls', 'product_parts', 'breadcrumb_categories', 'category_tags'):
            try:
                df[key] = df[key].apply(lambda x: str(x))
            except KeyError:
                continue

        # Generate report and return items
        self._process_report = self._generate_report(df.to_dict(orient='records'))
        return df.fillna(0).to_dict(orient='records')

    def _generate_report(self, items):
        df = pd.DataFrame(items)
        total_items = len(df)
        data_count = ((df.notnull()) & (df.applymap(lambda x: x != ''))).sum()
        data_count = data_count / total_items * 100
        report = {'total_products': total_items}
        for key, value in data_count.items():
            report[key] = int(value)

        return report

    def _translate_description_fields(self, item):
        """
        Translate description fields from SR to RU and EN.
        """

        self._translate_description_to_lang(item, 'ru')
        self._translate_description_to_lang(item, 'en')

    def _translate_description_to_lang(self, item, lang):
        """
        Translates descriptions to target language.
        """

        if lang not in ('ru', 'en'):
            raise ValueError("Language has to be either 'ru' or 'en'.")

        try:
            montikea_resp = self._montikea_client.get(item['product_id'], locale=lang)
        except Exception as e:
            message = f"Can't fetch RU data from montikea.com because {type(e).__name__}: {e}"
            print(message)
            item[f'product_description_{lang}'] = ''
            item[f'product_long_description_{lang}'] = ''
        else:
            try:
                montikea_soup = BeautifulSoup(montikea_resp.text, features='html.parser')
                item[f'product_description_{lang}'] = montikea_soup.find(
                    'div',
                    {'class': 'product__info'}
                ).find('p').text
                item[f'product_long_description_{lang}'] = montikea_soup.find(
                    'div',
                    {'class': 'product__description'}
                ).text.strip()
            except Exception as e:
                message = f"Can't parse RU description data from montikea.com because {type(e).__name__}: {e}"
                print(message)
                item[f'product_description_{lang}'] = ''
                item[f'product_long_description_{lang}'] = ''

    def get_report(self):
        """
        Returns report of processed items and current database state.
        """
        storage_service = StorageService()
        report = {
            'crawl_report': self._process_report,
            'database_report': self._generate_report(storage_service.get_all())
        }

        return report
