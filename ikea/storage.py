"""
Handles database and disk storage.
"""

import os
import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm
from sqlalchemy import create_engine, Column, String, Integer, Float, Boolean
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base


load_dotenv('config.env', override=True)
mysql_url = os.getenv('MYSQL_URL')
table_name = os.getenv('TABLE_NAME')
engine = create_engine(mysql_url, pool_pre_ping=True)
Session = sessionmaker(bind=engine)


class Product(declarative_base()):
    __tablename__ = table_name

    product_id = Column(String, primary_key=True)
    product_name = Column(String)
    price = Column(Integer)
    price_rs = Column(Float)
    price_hr = Column(Float)
    product_description = Column(String)
    product_long_description = Column(String)
    product_description_ru = Column(String)
    product_long_description_ru = Column(String)
    product_description_en = Column(String)
    product_long_description_en = Column(String)
    main_image_url = Column(String)
    other_image_urls = Column(String)
    product_url_ikea = Column(String)
    availability = Column(Boolean)
    num_of_packages = Column(Integer)
    multi_pack = Column(Boolean)
    product_parts = Column(String)
    sum_volume = Column(Float)
    sum_weight = Column(Float)
    materials = Column(String)
    max_dimension = Column(Float)
    breadcrumb_categories = Column(String)
    category_tags = Column(String)
    modified_date = Column(String)


class StorageService:

    def to_csv(self, products, name='ikea_rs_products.csv'):
        # Convert to Pandas data frame and save as CSV
        df = pd.DataFrame(products)
        unique_products = df.drop_duplicates(subset=['product_id'])
        unique_products.to_csv(name, index=False, encoding='utf-8-sig')

    def get_diff(self, products):
        """
        Splits products into existing and new.
        """

        session = Session()
        existing_products = []
        new_products = []
        for product in tqdm(products, desc="Determining diff"):
            if session.query(Product).filter(Product.product_id == product['product_id']).first():
                existing_products.append(product)
            else:
                new_products.append(product)

        session.close()
        return existing_products, new_products

    def upsert(self, products):
        """
        Adds new and updates existing products.
        """

        session = Session()
        for product in tqdm(products, desc="Adding items"):
            try:
                row = session.query(Product).filter(Product.product_id == product['product_id']).first()
            except Exception as e:
                message = f"Error fetching data from database for product_id {product['product_id']}, {type(e).__name__}: {e}"
                print(message)
                continue

            try:
                for key, value in product.items():
                    setattr(row, key, value)
            except AttributeError:
                try:
                    # Row doesn't exist, so we add it
                    new_row = Product(**product)
                    session.add(new_row)
                except Exception as e:
                    message = f"Error adding product with product_id {product['product_id']}, {type(e).__name__}: {e}"
                    print(message)

        session.commit()
        session.close()


    def get_all(self):
        """
        Returns all products.
        """

        session = Session()
        products = [product.__dict__ for product in session.query(Product).all()]
        for product in products:
            product.pop('_sa_instance_state')
        session.close()
        return products
