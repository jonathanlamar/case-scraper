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
        self.openAndPrefillSearchPage()

        locationSelect = Select(self.driver.find_element_by_id("Location_ID"))
        # Skipping first element because either
        # 1. There is only one option and we don't need to select it, or
        # 2. There are multiple options, but the first the blank "select none"
        #    fake option.
        locations = [x.text for x in locationSelect.options[1:]]

        # Loop over court rooms
        locationDockets = []
        for location in locations:
            self.debugLog('In loop.  location = ' + location)
            locationDocket = self.scrapeDataForOneLocation(location)
            locationDocket['court_location'] = location
            locationDockets.append(locationDocket)

        self.debugLog('Done with loop.')

        return pd.concat(locationDockets)

    def openAndPrefillSearchPage(self):
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

    def scrapeDataForOneLocation(self, location):
        handle = self.openDocketInTab(location)

        # Switch to new results tab
        self.driver.switch_to.window(handle)

        # Kinda a hacky way to ensure all results load on the page.
        # Even with the explicit wait below, it was not waiting long
        # enough for pages with lots of results.
        time.sleep(5)

        WebDriverWait(self.driver, 5).until(
            EC.visibility_of_all_elements_located(
                (By.CSS_SELECTOR, '#dockettable tr'))
        )

        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        results = soup.find('table', id='dockettable')
        table = pd.read_html(str(results), header=0)

        # pulls a weird singleton list containing a dataframe for some reason.
        assert len(table) == 1

        allCasesDf = table[0]
        self.debugLog("Pulled %d records from the %s docket"
                      % (allCasesDf.shape[0], self.county))
        fedCases = allCasesDf[allCasesDf['Hearing Type'] == 'FED Hearing']
        fedCases = (
            fedCases
            .rename(columns={'Case #': 'case_number'})
            .groupby('case_number', as_index=False)
            .agg(lambda x: '; '.join(set(x)))
        )

        fedCases['scraped_on'] = str(datetime.date.today())

        return fedCases

    def openDocketInTab(self, location):
        self.openAndPrefillSearchPage()

        locationSelect = Select(self.driver.find_element_by_id("Location_ID"))
        locationSelect.select_by_visible_text(location)

        self.debugLog('In openDocketInTab.  Looking for submitform.')

        submitCandidates = [x for x in self.driver.find_elements_by_name(
            'submitform') if x.is_displayed()]
        assert len(submitCandidates) == 1
        self.debugLog('Found submitform.')
        submitCandidates[0].click()

        allPagesLink = WebDriverWait(self.driver, 10).until(
            EC.element_to_be_clickable((By.LINK_TEXT, "Print All Pages")))
        self.debugLog('done waiting.')

        allPagesLink.click()

        # FIXME: This is error prone.
        # Return handle for the new tab
        return self.driver.window_handles[-1]

    def closeDriverAndIngest(self):
        if not self.debug:
            sheetsIngest = SheetsIngest(
                serviceAccountConfigLoc=os.getenv('GOOGLE_TOKEN'))

            # FIXME:
            # sheetsIngest.ingestNewBatchAndUpload(
            #     newlyScrapedCases=self.fedCases,
            #     countySheetId=ColoradoCountyScraper.countySheetIds[self.county])
        self.driver.quit()


if __name__ == '__main__':
    scraper = ColoradoCountyScraper('Boulder County', debug=True)
    df = scraper.scrape()
