"""
All sorts of IKEA related stuff.
"""

import json
import networkx as nx
import cyrtranslit as cyrt
from google.cloud import translate_v2 as translate


class CategoryBuilder:
    """
    Builds category graph from IKEA's category structure.
    """

    _start_point = 'https://www.ikea.com/rs/sr/'

    def __init__(self):
        self._depth = 1
        self.graph = nx.DiGraph()

    def _pairs_of_neighbors(self, lst):
        """
        Create pairs of neighboring elements.

        :param lst: list of elements to convert to list of pairs
        :return: list of tuples containing neighboring pairs
        """

        return [(lst[i], lst[i + 1]) for i in range(len(lst) - 1)]

    def build_from_products(self, products):
        """
        Builds category graph using the categoryPath of individual products.

        :param products: list of IKEA products
        :return: networkx graph
        """

        for product in products:
            # Add top node (products | Proizvodi)
            full_path = [{'key': 'products', 'name': 'Proizvodi'}]
            full_path.extend(product['category_path'])
            path = self._pairs_of_neighbors([node['key'] for node in full_path])
            self.graph.add_edges_from(path)
            for node in full_path:
                self.graph.nodes[node['key']]['name'] = node['name']

    def _graph_to_nested_dict(self, graph, node):
        """
        Recursively converts networkx DiGraph tree to nested dictionary.
        """

        if len(list(graph.successors(node))) == 0:
            return {'name': graph.nodes[node]['name'],
                    'key': node}
        else:
            return {'name': graph.nodes[node]['name'],
                    'key': node,
                    'children': [self._graph_to_nested_dict(graph, n) for n in graph.successors(node)]}

    def to_json(self, path):
        with open(path, 'w', encoding='utf-8') as json_file:
            json.dump(self._graph_to_nested_dict(self.graph, 'products'), json_file, ensure_ascii=False, indent=4)


def translate_text(string_text_to_translate, string_target_language, string_source_language='sr'):
    """
    This function translates text using google cloud translation API. Limitation is 128 string per request.

    :param: string_text_to_translate: text that needs to be translated. It can consist of multiple words
    :param: string_target_language: language in which the given string will be translated
    :param: string_source_language: source language of a text to be translated (by default it is Serbian)
    :return: translated text
    """

    # checking if the parameter for target language is valid
    if string_target_language not in ('ru', 'en'):
        raise ValueError("Parameter string_target_language must be either 'ru' or 'en'.")

    # if the translation was unsuccessful the following dictionary contains replacement strings
    # depending on the target language
    dict_unsuccessful_translation = {'en': "Unfortunately, translation for this article is not available.",
                                     'ru': "К сожалению, перевод этого продукта недоступен."}
    try:
        # initialize the client
        client = translate.Client()
        # translating text using google cloud translate API
        translated_text = client.translate(values=string_text_to_translate,
                                           target_language=string_target_language,
                                           source_language=string_source_language)
        # returning the translated text if it was successful
        return translated_text["translatedText"]
    except Exception as e:
        print(f"Error: Translation of '{string_text_to_translate}' was unsuccessful because of {type(e).__name__} : {e}")
        return dict_unsuccessful_translation[string_target_language]


def replace_swed_chars_with_russian(string_russian_word):
    """
    *Not used at the moment
    This function replaces characters specific to Swedish alphabet (e.g. "Ä": "Э", "Ö": "У", "Å": "O").

    :param: string_russian_word: this word potentially includes characters which are not part of the Russian alphabet
    :return: russian word with atypical characters replaced, if there were any
    """
    dict_characters_swe_ru = {"Ä": "Э", "Ö": "У", "Å": "O"}
    for key, val in dict_characters_swe_ru.items():
        string_russian_word = string_russian_word.replace(key, val)
    return string_russian_word


def transliterate_swedish_names_to_russian(string_product):
    """
    *Not used at the moment
    This function transliterates words in latin to Russian alphabet without translating it.
    (e.g. "VIMLE": "ВИМЛЕ", "PÄRUP": "ПAРУП")
    It also replaces Swedish special characters with Russian alphabet (e.g. "Ä": "A", "Ö": "O", "Å": "A")

    *Note: At this moment there is no need for this function. On 02. 03. 2024. Ceca told Dusan that there is no need to
    translate Swedish names of product to Russian language, but it is enough to show them in latin.

    :param: string_product: product in latin
    :return: product transliterated in cyrilic
    """
    modified_string_product = replace_swed_chars_with_russian(cyrt.to_cyrillic(string_product, 'ru'))
    return modified_string_product


def translate_single_product(dict_product):
    """
    This function receives product in a form of a dictionary, translates its product_name, product_description,
    product_long_description to English and adds corresponding values in Russian language to new keys: product_name_ru,
    product_description_ru, product_long_description_ru

    :param: dict_product: product which has defined values for keys: product_name, product_description, product_long_description
    :return: product in a form of a dictionary with translated values of mentioned keys to English and Russian
    """

    # translating product name
    dict_product["product_name_ru"] = dict_product["product_name"]
    dict_product["product_name_en"] = dict_product["product_name"]
    # translating product description
    dict_product["product_description_ru"] = dict_product.get("product_description_ru", translate_text(dict_product["product_description"], 'ru'))
    dict_product["product_description_en"] = dict_product.get("product_description_en", translate_text(dict_product["product_description"], 'en'))
    # translating product long description
    dict_product["product_long_description_ru"] = dict_product.get("product_long_description_ru", translate_text(dict_product["product_long_description"], 'ru'))
    dict_product["product_long_description_en"] = dict_product.get("product_long_description_en", translate_text(dict_product["product_long_description"], 'en'))

    # returning the dictionary with translated values
    return dict_product
