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
import random
import concurrent.futures
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

# load .env
load_dotenv(override=True)

# logger
cur_dir = os.path.dirname(os.path.abspath(__file__))
log_file_path = os.path.join(cur_dir, 'fpds_scrape.log')
logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    filemode='a',  # 'a' for append, 'w' for overwrite
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# db credentials
DB_USER = "postgres"
DB_HOST = "db.byhpfvdicvtmwnuwrbtp.supabase.co"
DB_PORT = 6543
DB_NAME = "postgres"
DB_PASSWORD = os.getenv("DB_PASSWORD")


def camel_to_snake(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1)
    return s2.lower()

def insert_db_batch(batch_data):
    if not batch_data:
        return
    
    try:
        with psycopg2.connect(
            user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT, dbname=DB_NAME, sslmode="require"
        ) as conn:
            with conn.cursor() as cur:
                # Prepare data for batch insertion and award updates
                insert_records = []
                update_awards = []
                for item in batch_data:
                    award_id = item['award_id']
                    
                    processed_item = {"award_id": award_id}
                    
                    for key, value in item['json_data'].items():
                        if isinstance(value, str) and (value.startswith('$') or value.startswith('-')):
                            processed_item[camel_to_snake(key)] = value.replace("$", "").replace(",", "")
                        elif key == "idvNumberOfOffersReceived":
                            if value is not None:
                                processed_item["number_of_offers_received"] = value
                        elif key != "idvNumberOfOffersReceived" and key != "award_id":
                            processed_item[camel_to_snake(key)] = value
                    insert_records.append(processed_item)

                # Batch insert into scraped_data_fpds
                if insert_records:
                    columns = list(insert_records[0].keys())
                    
                    # Construct a list of tuples for execute_batch
                    data_to_insert = [tuple(record.values()) for record in insert_records]
                    
                    insert_query_template = sql.SQL(
                        "INSERT INTO scraped_data_fpds_test ({columns}) VALUES ({values}) RETURNING id, award_id"
                    ).format(
                        columns=sql.SQL(', ').join(map(sql.Identifier, columns)),
                        values=sql.SQL(', ').join(sql.Placeholder() * len(columns))
                    )
                    extras.execute_batch(cur, insert_query_template, data_to_insert)
                    
                    # Collect returned fpds_id and award_id for updates
                    update_data = cur.fetchall()

                # Batch update u_awards
#                if update_data:
 #                   award_query_template = sql.SQL("UPDATE u_awards SET fpds_id=%s WHERE award_number=%s;")
  #                  extras.execute_batch(cur, award_query_template, update_data)
        
    except Exception:
        tracer = traceback.format_exc()
        logging.error(f"Error inserting batch into database, stacktrace: {tracer}")
        raise

def scrape_award_data(award_id, keys_config, driver):
    try:
        driver.get('https://www.fpds.gov/ezsearch/search.do?indexName=awardfull&templateName=1.5.3&s=FPDS.GOV&q=' + award_id)
        
        # wait for page load up to 5 seconds
        wait = WebDriverWait(driver, 5)
        
        # conditions to wait for
        view_link_condition = EC.element_to_be_clickable((By.XPATH, "//a[@title='View']"))
        no_results_condition = EC.presence_of_element_located((By.XPATH, "//span[@class='warning_text' and text()='No Results Found.']"))

        # if fail to load "No results" page or "View" button, exit 
        try:
            wait.until(EC.any_of(view_link_condition, no_results_condition))
        except Exception:
            logging.error(f"Timed out waiting for search results for award {award_id}")
            return None

        # if found no results return None 
        if driver.find_elements(By.XPATH, "//span[@class='warning_text' and text()='No Results Found.']"):
            logging.info(f"No results found for award {award_id}")
            return None

        # else click thru to the second page
        view_link = driver.find_element(By.XPATH, "//a[@title='View']")
        view_link.click()
        
        # switch context to next page
        all_window_handles = driver.window_handles
        driver.switch_to.window(all_window_handles[-1])
        
        # wait for body to show up
        wait.until(EC.presence_of_element_located((By.TAG_NAME, 'body')))

        # get html in  lxml 
        html_to_scrape = driver.page_source
        soup = BeautifulSoup(html_to_scrape, 'lxml')

        # scrape all keys
        scraped_data = {}
        for key, config in keys_config.items():
            if config['type'] == 'input':
                element = soup.find('input', id=key)
                if element:
                    scraped_data[key] = element.get('value') if element.get('value') else None
                else: 
                    scraped_data[key] = None
            elif config['type'] == 'dropdown':
                element = soup.find('select', id=key)
                if element and element.find('option', selected=True):
                    text = element.find('option', selected=True).text.strip()
                    scraped_data[key] = text if text else None
                else: 
                    scraped_data[key] = None

            elif config['type'] == 'text':
                element = soup.find(id=key)
                if element:
                    ele = element.text.strip()
                    if ele == "":
                        ele = None
                    scraped_data[key] = ele
                else: 
                    scraped_data[key] = None

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
        logging.error(f"Error scraping award {award_id}, stacktrace: {tracer}")
        print(f'error scraping: {tracer}')
        return None
    finally:
        # close the tab and switch back to the original
        if len(driver.window_handles) > 1:
            driver.close()
            driver.switch_to.window(driver.window_handles[0])

def scrape_and_insert(award_id, keys_config, driver):
    
    scraped_data = scrape_award_data(award_id, keys_config, driver)
    # if scrape worked return data otherwise no null resultZs
    if scraped_data is not None:
        return {'award_id': award_id, 'json_data': scraped_data}
    return None


def main():    
    award_ids = [line.strip() for line in sys.stdin if line.strip()]
    if not award_ids:
        logging.info("No award IDs received, exiting...")
        sys.exit(0)
    
    # set up browser
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--log-level=3")
    options.page_load_strategy = 'eager' 
    
    batch_data_to_insert = []   
    
    # thread pool manager
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        driver = None
        try:
            driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
            
            # submit all scrape jobs to 
            futures = {executor.submit(scrape_and_insert, award_id, keys, driver): award_id for award_id in award_ids}
            
            # Process results as they become available
            for future in concurrent.futures.as_completed(futures):
                award_id = futures[future]
                try:
                    result = future.result()
                    if result:
                        batch_data_to_insert.append(result)
                        print('scraped: ', award_id)
                        logging.info(f"Successfully scraped award ID: {award_id}")
                    else:
                        print('no result: ',award_id )
                        logging.warning(f"Failed to scrape award ID: {award_id}")
                except Exception:
                    tracer = traceback.format_exc()
                    logging.error(f"Failed to process award ID {award_id}, skipping. Stacktrace: {tracer}")
                    
        except Exception:
            tracer = traceback.format_exc()
            logging.error(f"Failed to initialize webdriver for batch. Stacktrace: {tracer}")
        finally:
            if driver:
                driver.quit()

    # Perform batch insert outside the scraping loop
    insert_db_batch(batch_data_to_insert)
    logging.info(f"Batch insert completed for {len(batch_data_to_insert)} records.")

if __name__ == "__main__":
    main()









if __name__ == "__main__":
    main()

