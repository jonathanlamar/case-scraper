from bs4 import BeautifulSoup
from dotenv import load_dotenv
from ingest.airtable import airtable_create
from ingest.sheets_ingest import SheetsIngest
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.select import Select
from selenium.webdriver.support.ui import WebDriverWait
import datetime
import os
import pandas as pd
import requests as rq
import sys
import time

load_dotenv()

sheetsIngest = SheetsIngest(serviceAccountConfigLoc=os.getenv('GOOGLE_TOKEN'))

counties = [
    {
        "name": "Boulder County",
        "endpoint_id": "1PMKEv78YgnaoIL1lmg7bAuvCK63WhBU9QrLXlVeFE4s"
    },
]


def scrape_county(driver, county):
    """
    Scrape the state courts docket by county

    Args:
        county (dict): The county we are searching for. Any county will work so
        long as it matches what is in the DOM (and we do not need to select the
        courthouse)
        county.name (string): Name of the county
        county.endpoint_id (string): Google Sheet ID
    """
    url = "https://www.courts.state.co.us/dockets/index.cfm#results"

    driver.get(url)
    driver.implicitly_wait(100)

    # Wait for the <form id="DocketSearch"> element to load
    WebDriverWait(driver, 10).until(
        EC.visibility_of_element_located((By.XPATH, '//*[@id="DocketSearch"]'))
    )

    # Make docket selections
    Select(driver.find_element_by_id('County_ID')
           ).select_by_visible_text(county['name'])
    if county["name"] in ["Arapahoe County", "Boulder County"]:
        court_select = '//*[@id="Location_ID"]/option[1]'
        driver.find_element_by_xpath(court_select).click()
    Select(driver.find_element_by_id('datesearchtype')
           ).select_by_visible_text('1 Week')

    # Click the submit button
    driver.find_element_by_id('submitform').click()

    # Wait for the parent div to load for the Print All Pages link
    WebDriverWait(driver, 10).until(
        EC.visibility_of_element_located(
            (By.CSS_SELECTOR, '#docketresults > div.page-content-right'))
    )

    # Click the Print All Pages link
    # Print All Pages link from results page
    print_all_path = '//*[@id="docketresults"]/div[2]/span/a[2]'
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

    return downloadResultsToDataframe(driver)


def downloadResultsToDataframe(driver):
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

    return fed_cases


def closeDriverAndIngest(debug):
    if DEBUG:
        driver.quit()
    else:
        sheetsIngest.ingestNewBatchAndUpload(
            newlyScrapedCases=fed_cases,
            countySheetId=county['endpoint_id'])
        driver.quit()


if __name__ == '__main__':
    if os.getenv('SELENIUM_DRIVER'):
        if os.getenv('SELENIUM_DRIVER').lower() == 'chrome':
            driver = webdriver.Chrome()
        elif os.getenv('SELENIUM_DRIVER').lower() == 'firefox':
            driver = webdriver.Firefox()
    else:
        sys.exit(
            "Please set which web driver you're using for Selenium in your .env file using variable SELENIUM_DRIVER!")

    for county in counties:
        fed_cases = scrape_county(driver, county)
        fed_cases.to_csv(
            f'out/{county["name"].replace(" ", "_")}_{datetime.date.today().strftime("%Y-%m-%d")}.csv', index=False)
        closeDriverAndIngest(debug=True)
