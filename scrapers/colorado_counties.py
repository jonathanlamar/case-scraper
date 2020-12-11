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
from IPython import embed

load_dotenv()


class ColoradoCountyScraper:
    countySheetIds = {
        'Boulder County': "1PMKEv78YgnaoIL1lmg7bAuvCK63WhBU9QrLXlVeFE4s",
        'Denver County': "foooooo",  # For testing
    }
    stateCourtsUrl = "https://www.courts.state.co.us/dockets/index.cfm#results"

    def __init__(self, county, debug=False):
        self.county = county
        self.debug = debug
        self.fedCases = None

        if os.getenv('SELENIUM_DRIVER').lower() == 'chrome':
            self.driver = webdriver.Chrome()
        elif os.getenv('SELENIUM_DRIVER').lower() == 'firefox':
            self.driver = webdriver.Firefox()
        else:
            raise RuntimeError('Cannot find driver type in .env file.')

    def debugLog(self, promptText):
        print('!'*80)
        print(promptText)
        print('!'*80)

    def scrape(self):
        self.driver.get(ColoradoCountyScraper.stateCourtsUrl)
        self.driver.implicitly_wait(100)

        # Wait for the <form id="DocketSearch"> element to load
        WebDriverWait(self.driver, 10).until(
            EC.visibility_of_element_located(
                (By.XPATH, '//*[@id="DocketSearch"]'))
        )

        # Make docket selections
        (Select(self.driver.find_element_by_id('County_ID'))
         .select_by_visible_text(self.county))
        (Select(self.driver.find_element_by_id('datesearchtype'))
         .select_by_visible_text('1 Week'))
        locationSelect = Select(self.driver.find_element_by_id("Location_ID"))
        # Skipping first because either
        # 1. There is only one option and we don't need to select it, or
        # 2. There are multiple options, but the first the blank "select none"
        #    fake option.
        locations = [x.text for x in locationSelect.options[1:]]

        # Loop over court rooms
        locationDockets = []
        for location in locations:
            self.debugLog('In loop.  location = ' + location)
            handle = self.openDocketInTab(location)
            # locationDocket = self.scrapeDataForOneLocation(handle)
            # locationDocket['court_location'] = locationText
            # locationDockets.append(locationDocket)

        self.debugLog('Done with loop.')

        # return pd.concat(locationDockets)

    def openDocketInTab(self, location):
        locationSelect = Select(self.driver.find_element_by_id("Location_ID"))
        locationSelect.select_by_visible_text(location)

        self.debugLog('In openDocketInTab.  Looking for submitform.')
        embed()

        submitCandidates = [x for x in self.driver.find_elements_by_name(
            'submitform') if x.is_displayed()]
        assert len(submitCandidates) == 1
        self.debugLog('Found submitform.')
        submitCandidates[0].click()

        # Wait for the parent div to load for the Print All Pages link
        # WebDriverWait(self.driver, 10).until(
        #     EC.visibility_of_element_located(
        #         (By.CSS_SELECTOR, '#docketresults > div.page-content-right'))
        # )
        time.sleep(5)

        self.debugLog('done waiting.')

        # Click the Print All Pages link
        # Print All Pages link from results page
        print_all_path = '//*[@id="docketresults"]/div[2]/span/a[2]'
        self.driver.find_element_by_xpath(print_all_path).click()

        # Return handle for the new tab
        return self.driver.window_handles[-1]

    def closeDriverAndIngest(self):
        if not self.debug:
            sheetsIngest = SheetsIngest(
                serviceAccountConfigLoc=os.getenv('GOOGLE_TOKEN'))
            sheetsIngest.ingestNewBatchAndUpload(
                newlyScrapedCases=self.fedCases,
                countySheetId=ColoradoCountyScraper.countySheetIds[self.county])
        self.driver.quit()

    def scrapeDataForOneLocation(self, handle):
        # Switch to new results tab
        self.driver.switch_to.window(handle)

        # Kinda a hacky way to ensure all results load on the page.
        # Even with the explicit wait below, it was not waiting long
        # enough for pages with lots of results.
        time.sleep(5)

        # Wait for all table elements to load from results tab
        WebDriverWait(self.driver, 5).until(
            EC.visibility_of_all_elements_located(
                (By.CSS_SELECTOR, '#dockettable tr'))
        )

        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        # Results page results table
        results = soup.find('table', id='dockettable')

        # Read results into pandas df
        table = pd.read_html(str(results), header=0)
        print(
            f"Pulled {len(table[0])} records from the {self.county} docket")

        # Filter for FED cases only
        fedCases = table[0].loc[table[0]["Hearing Type"] == "FED Hearing"]
        fedCases = (
            fedCases
            .rename(columns={'Case #': 'case_number'})
            .groupby('case_number', as_index=False)
            .agg(lambda x: '; '.join(set(x)))
        )

        # Save the results to CSV
        fedCases['scraped_on'] = str(datetime.date.today())

        return fedCases


if __name__ == '__main__':
    scraper = ColoradoCountyScraper('Denver County', debug=True)
    scraper.scrape()
