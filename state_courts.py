import pandas as pd
import requests as rq
import time
import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.select import Select
import os
import sys
from dotenv import load_dotenv

from ingest.airtable import airtable_create
from ingest.sheets_ingest import SheetsIngest

# DEBUG set to True will run the scraper and save a CSV, but will not send the data to google sheets
DEBUG = False

load_dotenv()

sheetsIngest = SheetsIngest(serviceAccountConfigLoc=os.getenv('GOOGLE_TOKEN'))

"""
counties = [
    {
        "name": "Jefferson County",
        "endpoint_id": "1IDcPcPX3_f-wKoJJLmB2IHqtpkqsJNvrAZGaEiMMulk"
    },
    {
        "name": "Arapahoe County",
        "endpoint_id": "1UkW3n-5TGxiHgV2UKWA-eXa3CjcRwRizh1K07_lITu4"
    },
    {
        "name": "Adams County",
        "endpoint_id": "1qPYrp6wPgrg3ZM4DLKwt-u3rdxC8aJ8HAdRyhTzukTA"
    },
    {
        "name": "Douglas County",
        "endpoint_id": "1t4Aw6x71CKBF-LFxFDc6SDYVQI7iozlitSgVZr5T010"
    }
]
"""
counties = [
    {
        "name": "Jefferson County",
        "endpoint_id": "1WObDCxp50Tk11aMQrHp3oF-uNd1vU7U_JWIbBzR0q08"
    },
]


def scrape_county(county):
    """
    Scrape the state courts docket by county

    Args:
        county (dict): The county we are searching for. Any county will work so long as it matches what is in the DOM (and we do not need to select the courthouse)
        county.name (string): Name of the county
        county.endpoint_id (string): Google Sheet ID
    """
    url = "https://www.courts.state.co.us/dockets/index.cfm#results"

    if os.getenv('SELENIUM_DRIVER'):
        if os.getenv('SELENIUM_DRIVER').lower() == 'chrome':
            driver = webdriver.Chrome()
        elif os.getenv('SELENIUM_DRIVER').lower() == 'firefox':
            driver = webdriver.Firefox()
    else:
        sys.exit(
            "Please set which web driver you're using for Selenium in your .env file using variable SELENIUM_DRIVER!")

    driver.get(url)
    driver.implicitly_wait(100)

    # Wait for the <form id="DocketSearch"> element to load
    WebDriverWait(driver, 10).until(
        EC.visibility_of_element_located((By.XPATH, '//*[@id="DocketSearch"]'))
    )

    counties_soup = BeautifulSoup(driver.page_source, 'html.parser')

    # Create a dict of counties and their values we can use to determine their order in the DOM
    counties_dict = {}
    for option in counties_soup.select('select#County_ID option'):
        counties_dict[f"{option.text.strip()}"] = option["value"].strip()

    # Select County from the county dropdown
    county_select = f'//*[@id="County_ID"]/option[{list(counties_dict.keys()).index(county["name"])+1}]'
    #county_select = f'//*[@id="County_ID"]/option[4]'
    if county["name"] == "Arapahoe County":
        court_select = '//*[@id="Location_ID"]/option[4]'
    # Select 1 Week from date dropdown
    date_range = '//*[@id="datesearchtype"]/option[4]'
    # Submit button
    submit_btn = '//*[@id="submitform"]'
    # Print All Pages link from results page
    print_all_path = '//*[@id="docketresults"]/div[2]/span/a[2]'

    # Click the county from the select dropdown
    driver.find_element_by_xpath(county_select).click()
    if county["name"] == "Arapahoe County":
        driver.find_element_by_xpath(court_select).click()
    # Click the date range we're searching for
    driver.find_element_by_xpath(date_range).click()
    # Click the submit button
    driver.find_element_by_xpath(submit_btn).click()

    # Wait for the parent div to load for the Print All Pages link
    WebDriverWait(driver, 10).until(
        EC.visibility_of_element_located(
            (By.CSS_SELECTOR, '#docketresults > div.page-content-right'))
    )

    # Click the Print All Pages link
    driver.find_element_by_xpath(print_all_path).click()
    # Switch to new results tab
    driver.switch_to.window(driver.window_handles[1])

    # Kinda a hacky way to ensure all results load on the page.
    # Even with the explicit wait below, it was not waiting long
    # enough for pages with lots of results.
    time.sleep(5)

    # Wait for all table elements to load from results tab
    WebDriverWait(driver, 5).until(
        EC.visibility_of_all_elements_located(
            (By.CSS_SELECTOR, '#dockettable tr'))
    )

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    # Results page results table
    results = soup.find('table', id='dockettable')

    # Read results into pandas df
    table = pd.read_html(str(results), header=0)
    print(f"Pulled {len(table[0])} records from the {county['name']} docket")
    # Filter for FED cases only
    fed_cases = table[0].loc[table[0]["Hearing Type"] == "FED Hearing"]
    fed_cases = (
        fed_cases
        .rename(columns={'Case #': 'case_number'})
        .groupby('case_number', as_index=False)
        .agg(lambda x: '; '.join(set(x)))
    )

    # Save the results to CSV
    fed_cases['scraped_on'] = str(datetime.date.today())
    fed_cases.to_csv(
        f'out/{county["name"].replace(" ", "_")}_{datetime.date.today().strftime("%Y-%m-%d")}.csv', index=False)

    if DEBUG:
        driver.quit()
    else:
        sheetsIngest.ingestNewBatchAndUpload(
            newlyScrapedCases=fed_cases,
            countySheetId=county['endpoint_id'])
        driver.quit()


if __name__ == '__main__':
    for county in counties:
        scrape_county(county)
