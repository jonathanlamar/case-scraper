from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.select import Select
from selenium.webdriver.support.ui import WebDriverWait
import argparse
import pandas as pd
import re
import requests as rq
import time

################################################################################
# WARNING: This does not work.
# This is a work in progress that requires access to the state courts
# public terminal with multi-factor access.
# There's an issue with handling cookies in selenium that needs to be worked out
# before this is ready to go. Other planned updates, if possible:
#   - Pull complaint file and upload to google drive
#   - Link complaint file to the case so organizers can pull address from there
################################################################################

parser = argparse.ArgumentParser()

# For testing purposes only
t = 'https://www.jbits.courts.state.co.us/publicAccess/web/case/71/C/2020/C/37588'
case = 'C712020C37588'

parser.add_argument('-i', '--sess_id', help='JSESSIONID cookie.')
args = parser.parse_args()


class TerminalCaseScraper:
    def __init__(self, sess_id=None):
        self.sessId = sess_id
        self.soup = None
        self.urlPrefix = 'https://www.jbits.courts.state.co.us/publicAccess/web/case'

    def parse_case(self, case_num):
        """Parse the case number into elements for URL

        Args:
            case_num (string): The case number we are going to search for

        Returns:
            dict: dictionary of each element we need to extract from the case number
        """
        # pylint: disable=anomalous-backslash-in-string
        county_id = re.findall("C(\S{2})", case_num)[0]
        case_year = re.findall("20\d{2}", case_num)[0]
        case_id = re.findall("C(\d+)", case_num)[1]

        params_obj = {
            "county": county_id,
            "year": case_year,
            "id": case_id
        }
        return params_obj

    def pull_case(self, case_num):
        case_params = self.parse_case(case_num)

        url = f"{self.urlPrefix}/{case_params['county']}/C/{case_params['year']}/C/{case_params['id']}"
        print(url)
        print(self.sessId)

        cookies = {
            'name': 'JSESSIONID',
            'value': self.sessId,
            'path': '/publicAccess',
            'domain': 'www.jbits.courts.state.co.us',
            'secure': True,
            'httpOnly': True
        }

        fp = webdriver.FirefoxProfile()
        #fp.set_preference("network.cookie.cookieBehavior", 2)

        driver = webdriver.Firefox(firefox_profile=fp)
        driver.get('https://www.jbits.courts.state.co.us/publicAccess/web/search')
        # time.sleep(2)
        print(driver.get_cookie('JSESSIONID'))
        # driver.delete_cookie('JSESSIONID')
        driver.delete_all_cookies()
        driver.add_cookie(cookies)
        # print(driver.get_cookie('JSESSIONID'))
        print(driver.get_cookies())
        # time.sleep(1)
        driver.get(url)
        driver.implicitly_wait(100)
        print(driver.get_cookies())

        # Wait for the <table id="caseHistoryTable"> element to load
        WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located(
                (By.XPATH, '//*[@id="caseHistoryTable"]'))
        )

        caption_select = '//*[@id="shortCaption"]'
        time.sleep(2)

        driver.find_element_by_xpath(caption_select).click()

        # Wait for the <table id="partyTable"> element to load
        WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located(
                (By.XPATH, '//*[@id="partyTable"]'))
        )

        self.soup = BeautifulSoup(driver.page_source, 'html.parser')

        party_table = self.soup.find('table', id='partyTable')
        print(party_table)

        party_df = self.case_to_df(case_num, party_table)

        return party_df

    def case_to_df(self, case_num, html_obj):
        table = pd.read_html(str(html_obj), header=0)

        table = table[0]

        table.set_index('Party Type', inplace=True)

        data = {
            'case': [case_num],
            'plaintiff': table.loc['Plaintiff', 'Party Name'],
            'defendant': table.loc['Defendant', 'Party Name']
        }
        case_df = pd.DataFrame(data=data)
        return case_df

    # def generate_terminal_df(case_nums)

    # def join_dfs(county_df)


if __name__ == '__main__':
    df = pd.read_csv('./out/Jefferson_County_2020-08-16-1.csv')
    print(df.head())
    terminal = TerminalCaseScraper(sess_id=args.sess_id)

    html = terminal.pull_case(case)

    print(html)
    print(html.columns)
    print(html['defendant'])

