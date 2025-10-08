#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Oct  5 14:40:32 2025

@author: algo-env
"""

import time
import sys
import os
import re
import traceback
import logging
import psycopg2
from openai import OpenAI
from psycopg2 import sql
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

plain_text_keys = [
    "displayAwardType", 
    "displayStatus",
    "displayClosedStatus",
    "displayIDVType",
]

drop_down_keys = ["foreignFunding", "typeOfContractPricing"]

# isolate duplicate keys to save time
duplicate_id_keys = ["totalEstimatedOrderValue"]

# load env 
load_dotenv()

# open router
client = OpenAI(
    base_url="https://openrouter.ai/api/v1",
    # Required for OpenRouter
    default_headers={"HTTP-Referer": "http://localhost:5000"}
)

try:
    html_to_scrape,award_id = None, sys.argv[1] # in case main scraper fails, scrape html object
except Exception:
    logging.error(f"{award_id}, no award id passed")
    print(f"{award_id}, no award id passed, exiting ...")
    os._exit(1)

#supabase
DB_USER = "postgres.krqimyqweygsvlzjakyh"
DB_HOST = "aws-1-us-east-2.pooler.supabase.com"
DB_PORT = 6543
DB_NAME = "postgres"
DB_PASSWORD = os.getenv("DB_PASSWORD")

conn = psycopg2.connect(
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT,
    dbname=DB_NAME
)


def camel_to_snake(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1)
    return s2.lower()

def insert_json_db(json_data, award_id):
    try:
        with psycopg2.connect(user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT, dbname=DB_NAME) as conn:
            with conn.cursor() as cur:
               
                # remove dollar sign and comma
                dollar_value_keys = [k for k, v in json_data.items() if v and isinstance(v, str) and (
                    v.startswith('$') or v.startswith('-'))]
                for key in dollar_value_keys:
                    json_data[key] = json_data[key].replace("$", "").replace(",", "")

                # add the procurement identifier
                json_data = {"award_id": award_id, **json_data}
                
                # place idv offers in # offers received               
                if "idvNumberOfOffersReceived" in json_data:
                    if json_data["idvNumberOfOffersReceived"] != None:
                        json_data["numberOfOffersReceived"] = json_data["idvNumberOfOffersReceived"]      
                    # merge two types into one. delete idv
                    del json_data["idvNumberOfOffersReceived"]
                        
                # convert html id's to db columm names
                columns = [camel_to_snake(item) for item in json_data.keys()]
                values = list(json_data.values())

                insert_query = sql.SQL(
                    "INSERT INTO scraped_data7 ({columns}) VALUES ({values})"
                ).format(
                    columns=sql.SQL(', ').join(map(sql.Identifier, columns)),
                    values=sql.SQL(', ').join(sql.Placeholder() * len(columns))
                )
                cur.execute(insert_query, values)
                
                # create an update statement on either of the two tables
                
                
                
                
    except Exception:
        tracer = traceback.format_exc()
        logging.error(f"Error inserting into database for award {award_id}, stacktrace: {tracer}")
        raise

def scrape_award_data(driver, award_id, keys_config):
    # navigate first website
    driver.get('https://www.fpds.gov/ezsearch/search.do?indexName=awardfull&templateName=1.5.3&s=FPDS.GOV&q=' + award_id)
    wait = WebDriverWait(driver, 10)
    
    original_window = driver.window_handles[0] # first page
    
    # click thru to second
    view_link = wait.until(
        EC.element_to_be_clickable((By.XPATH, "(//a[@title='View'])[1]"))
    )
    view_link.click()
    time.sleep(1)
    
    # after having clicked thru, get both window names
    all_window_handles = driver.window_handles        
    
    second_window = None
    for window_handle in all_window_handles:
        if window_handle != original_window:
            second_window = window_handle
            break
        
    driver.switch_to.window(original_window)
    driver.close()
     
    # Switch back to the new window for scraping
    driver.switch_to.window(second_window)
            
    
    # get html to scrape
    wait.until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
    html_to_scrape = driver.page_source
    soup = BeautifulSoup(html_to_scrape, 'lxml')

    scraped_data = {}
    for key, config in keys_config.items():
        try:
            if config['type'] == 'input':
                element = soup.find('input', id=key)
                if element:
                    ele = element.get('value') 
                    if ele == "":
                        ele = None # nullify
                    scraped_data[key] = ele
            
            elif config['type'] == 'dropdown':
                element = soup.find('select', id=key)
                if element and element.find('option', selected=True):
                    ele = element.find('option', selected=True).text.strip()
                    if ele == "":
                        ele = None
                    scraped_data[key] = ele
            
            elif config['type'] == 'text':
                element = soup.find(id=key)
                if element:
                    ele = element.text.strip()
                    if ele == "":
                        ele = None
                    scraped_data[key] = ele 
            
            elif config['type'] == 'duplicate_id':
                elements = soup.find_all(id=key)
                # Assuming you need the last one for the total value
                for element in elements:
                    if element:  
                        if element.name == 'input':
                            scraped_data[key] = element.get('value')
                    else:
                        scraped_data[key] = None
                    
        except Exception as e:
            logging.warning(f"Failed to scrape key {key} for award {award_id}, error: {e}")
            scraped_data[key] = None
    
    return scraped_data

def backup_scraper_on_error():
    # add slack notification here
    prompt = f"""
        Given the following HTML content, extract the specified fields as a JSON object.
    
        HTML content:
        ---
        {html_to_scrape}
        ---
    
        Extract the following fields into a JSON object: {', '.join(keys)}.
        Provide only the JSON object in your response.
    """
    try:
    
        response = client.chat.completions.create(
            model="openai/gpt-4o",
            messages=[
                  {"role": "system", "content": "You are a data extraction bot. You convert HTML snippets into JSON objects based on user instructions."},
                  {"role": "user", "content": prompt}
            ],
            response_format={"type": "json_object"}
        )
        
        with conn.cursor() as cur:
            sql = """
                INSERT INTO scraped_data7 (award_id, fail_safe_data_collection) 
                VALUES (%s, %s);
            """
            try:
                cur.execute(sql, (award_id, str(response),))
                conn.commit()
            except Exception as e:
                print('error secondary sql insert', e)
       
    except Exception:
        tracer = traceback.format_exc()
        logging.error(f"error on secondary scraper, stacktrace: {tracer}")
    

if __name__ == '__main__':
    try:
        # web drivers
        options = Options()
        options.add_argument('--no-sandbox')
        options.add_argument('--headless')
        options.add_argument('--disable-dev-shm-usage')
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        
        scraped_data_dict = scrape_award_data(driver, award_id, keys)
        
        insert_json_db(scraped_data_dict, award_id)
        
        logging.info(f"finished scraping for award {award_id}")
        
    except Exception:
        tracer = traceback.format_exc()
        logging.error(f"Primary scraper failed for award {award_id}: no result " )
        #backup_scraper_on_error() turn off for efficiency, dont record on null awards
        
    finally:
        if 'driver' in locals() and driver:
            driver.quit()
