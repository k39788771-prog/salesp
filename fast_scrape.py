#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 10 10:34:42 2025
@author: algo-env
"""

import sys
import os
import re
import traceback
import logging
import psycopg2
import time
from psycopg2 import sql, extras
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

keys = {
    "displayIDVType": {"type": "text"},
    "displayAwardType": {"type": "text"},
    "displayStatus": {"type": "text"},
    "displayClosedStatus": {"type": "text"},
    "typeOfContractPricing": {"type": "dropdown"},
    "agencyID": {"type": "input"},
    "modNumber": {"type": "input"},
    "transactionNumber": {"type": "input"},
    "vendorName": {"type": "input"},
    "principalPlaceOfPerformanceName": {"type": "input"},
    "placeStateCode": {"type": "input"},
    "placeOfPerformanceZIPCode": {"type": "input"},
    "placeOfPerformanceZIPCode4": {"type": "input"},
    "displayApprovedDate": {"type": "text"},
    "obligatedAmount": {"type": "input"},
    "baseAndExercisedOptionsValue": {"type": "input"},
    "ultimateContractValue": {"type": "input"},
    "foreignFunding": {"type": "dropdown"},
    "feesPaidForUseOfService": {"type": "input"},
    "fundingRequestingOfficeID": {"type": "input"},
    "fundingRequestingAgencyID": {"type": "input"},
    "contractingOfficeID": {"type": "input"},
    "contractingOfficeAgencyID": {"type": "input"},
    "contractingOfficeAgencyName": {"type": "input"},
    "contractingOfficeName": {"type": "input"},
    "fundingRequestingAgencyName": {"type": "input"},
    "fundingRequestingOfficeName": {"type": "input"},
    "numberOfOffersReceived": {"type": "input"},
    "principalNAICSCode": {"type": "input"},
    "NAICSCodeDescription": {"type": "input"},
    "productOrServiceCode": {"type": "input"},
    "productOrServiceCodeDescription": {"type": "input"},
    "signedDate": {"type": "input"},
    "effectiveDate": {"type": "input"},
    "lastDateToOrder": {"type": "input"},
    "solicitationDate": {"type": "input"},
    "totalEstimatedOrderValue": {"type": "duplicate_id"},
    "totalObligatedAmount": {"type": "input"},
    "totalUltimateContractValue": {"type": "input"},
    "baseAndAllOptionsValue": {"type": "input"},
    "idvNumberOfOffersReceived": {"type":"input"},
    "cageCode" : {"type": "input"},
}
plain_text_keys = ["displayAwardType", "displayStatus", "displayClosedStatus", "displayIDVType"]
drop_down_keys = ["foreignFunding", "typeOfContractPricing"]
duplicate_id_keys = ["totalEstimatedOrderValue"]

# Load environment variables once at the start
load_dotenv(override=True)

# logger
cur_dir = os.path.dirname(os.path.abspath(__file__))
log_file_path = os.path.join(cur_dir, 'fpds_scrape.log')

# Configure the basic settings for the logger
logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    filemode='a',  # 'a' for append, 'w' for overwrite
    format='%(asctime)s - %(levelname)s - %(message)s'
)

DB_USER = "postgres"
DB_HOST = "db.byhpfvdicvtmwnuwrbtp.supabase.co"
DB_PORT = 6543
DB_NAME = "postgres"
DB_PASSWORD = os.getenv("DB_PASSWORD")

def camel_to_snake(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1)
    return s2.lower()

def insert_db(json_data, award_id):
    try:
        with psycopg2.connect(
            user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT, dbname=DB_NAME, sslmode="require"
        ) as conn:
            with conn.cursor() as cur:
               
                processed_item = {"award_id": award_id}
                
                # remove dollar sign, make float & handle blank idv numbers, add to dict
                for key, value in json_data.items():
                    if isinstance(value, str) and (value.startswith('$') or value.startswith('-')):
                        processed_item[camel_to_snake(key)] = value.replace("$", "").replace(",", "")
                    elif key == "idvNumberOfOffersReceived":
                        if value is not None:
                            processed_item["number_of_offers_received"] = value
                    elif key != "idvNumberOfOffersReceived" and key != "award_id":
                        processed_item[camel_to_snake(key)] = value


                columns = list(processed_item.keys())
                values_tuple = tuple(processed_item.values())

                insert_query = sql.SQL(
                    "INSERT INTO scraped_data_fpds ({columns}) VALUES ({values}) RETURNING id"
                ).format(
                    columns=sql.SQL(', ').join(map(sql.Identifier, columns)),
                    values=sql.SQL(', ').join(sql.Placeholder() * len(columns))
                )
                cur.execute(insert_query, values_tuple)
                fpds_id = cur.fetchone()[0]

                #update award table
                award_query = sql.SQL("UPDATE u_awards SET fpds_id=%s WHERE award_number=%s;")
                cur.execute(award_query, (fpds_id, award_id))
                
                conn.commit()
                
    except Exception:
        tracer = traceback.format_exc()
        print("error inserting award id: ", award_id)
        logging.error(f"Error inserting into database for award {award_id}, stacktrace: {tracer}")
        raise

def scrape_award_data(award_id, keys_config, driver):
    try:
        driver.get('https://www.fpds.gov/ezsearch/search.do?indexName=awardfull&templateName=1.5.3&s=FPDS.GOV&q=' + award_id)
        wait = WebDriverWait(driver, 5)

        # conditions to wait for
        view_link_condition = EC.element_to_be_clickable((By.XPATH, "//a[@title='View']"))
        no_results_condition = EC.presence_of_element_located((By.XPATH, "//span[@class='warning_text' and text()='No Results Found']"))

        try:
            wait.until(EC.any_of(view_link_condition, no_results_condition))
        except Exception:
            return None

        if driver.find_elements(By.XPATH, "//span[@class='warning_text' and text()='No Results Found.']"):
            logging.info(f"No results found for award {award_id}")
            print(f"No results found {award_id}")
            return {}

        view_link = wait.until(
            EC.element_to_be_clickable((By.XPATH, "(//a[@title='View'])"))
        )
        view_link.click()
        
        all_window_handles = driver.window_handles
        driver.switch_to.window(all_window_handles[-1])
        
        wait.until(EC.presence_of_element_located((By.TAG_NAME, 'body')))

        html_to_scrape = driver.page_source
        soup = BeautifulSoup(html_to_scrape, 'lxml')

        scraped_data = {}
        for key, config in keys_config.items():
            if config['type'] == 'input':
                element = soup.find('input', id=key)
                if element:
                    scraped_data[key] = element.get('value') if element.get('value') else None
            elif config['type'] == 'dropdown':
                element = soup.find('select', id=key)
                if element and element.find('option', selected=True):
                    text = element.find('option', selected=True).text.strip()
                    scraped_data[key] = text if text else None
            elif config['type'] == 'text':
                element = soup.find(id=key)
                if element:
                    ele = element.text.strip()
                    if ele == "":
                        ele = None
                    scraped_data[key] = ele 
            elif config['type'] == 'duplicate_id':
                elements = soup.find_all(id=key)
                for element in elements:
                    if element:  
                        if element.name == 'input':
                            scraped_data[key] = element.get('value')
                    else:
                        scraped_data[key] = None
        
        return scraped_data

    except Exception:
        tracer = traceback.format_exc()
        print("error scraping award id: ", award_id)
        logging.error(f"Error scraping award {award_id}, stacktrace: {tracer}")
        return None
    finally:
        if len(driver.window_handles) > 1:
            driver.close()
            driver.switch_to.window(driver.window_handles[0])

def main():
    award_ids = [line.strip() for line in sys.stdin if line.strip()]
    if not award_ids:
        logging.info("No award IDs received, exiting...")
        sys.exit(0)
    
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1280,720")
    options.add_argument("--log-level=3") 
    options.page_load_strategy = 'eager' 
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)


    try:
        for award_id in award_ids:
            logging.info(f"Processing award ID: {award_id}")
       
            scraped_data = scrape_award_data(award_id, keys, driver)
            if scraped_data:
                insert_db(scraped_data, award_id)

    finally:
        driver.quit()


if __name__ == "__main__":
    try:
        main()
    except IndexError:
        logging.info('This program needs to be run with GNU Parallel.')
        sys.exit(1)
