from IPython import embed
from bs4 import BeautifulSoup
from functools import reduce
from itertools import cycle
from lxml.html import fromstring
from time import sleep
import pandas as pd
import requests


class PhoneNumberScraper:
    def __init__(self, namesAndCities):
        """__init__.

        Parameters
        ----------
        namesAndCities : pandas.DataFrame
            namesAndCities needs two columns, "Name" and "City"
        """

        self.namesAndCities = namesAndCities.dropna(subset=['Name'])

        # List of proxies to rotate through to bypass rate limit.
        self.proxies = cycle(PhoneNumberScraper.getProxies())

    def getPhoneNumbers(self, debug):
        """getPhoneNumbers.  Given a dataframe of names and cities, looks up
        phone numbers.
        """
        if 'Phone Number' not in self.namesAndCities.columns:
            self.updatePhoneNumbers(debug)

        return self.namesAndCities

    def updatePhoneNumbers(self, debug):
        """updatePhoneNumbers.  Scrape "most likely" phone number for each
        customer from truepeoplesearch.com
        """
        self.namesAndCities['Phone Number'] = self.namesAndCities.apply(
            lambda row: self.getNumbersForPeople(row[1], row[2], debug),
            axis=1,
            result_type='reduce')

    def getNumbersForPeople(self, nameList, city, debug):
        """getNumbersForPerson.  Given name and city, queries TruePeopleSearch
        for a list of all phone numbers.

        Parameters
        ----------
        nameList : string
            name(s) of people, pipe (|) separated
        city : string
            city the person presumably lives in
        """

        phoneNumberList = []
        names = [self.transformNameString(name)
                 for name in nameList.split("|")]

        for name in names:
            print('Finding numbers for %s.' % name)

            searchSoup = self.getSearchSoup(name, city, debug)

            try:
                detailsSoup = self.parseSearchSoupGetDetailsSoup(
                    searchSoup, debug)
            except RuntimeError:
                print('No hits found for %s.' % name)
                continue

            phoneNumberList += self.parseSoupGetPhoneNumbers(detailsSoup)

        return ", ".join([name + ': ' + number for number in phoneNumberList])

    def transformNameString(self, nameString):
        """transformNameString.  Converts "Last, First Middle" to "First Middle
        Last"

        Parameters
        ----------
        nameString : string
        """

        parts = nameString.split(",")

        # Move last name to the end and strip spaces.
        parts = [p.strip(" ") for p in parts[1:] + parts[:1]]

        return " ".join(parts)

    def getSearchSoup(self, name, city, debug):
        """getSearchUrl.  Returns url for searching for the name and city.

        Parameters
        ----------
        name :
            name of person
        city :
            city they live in (supposedly)
        """

        codedName = self.encode(name)
        codedCity = self.encode(city)

        url = (
            "https://www.truepeoplesearch.com/details?" +
            "name=" + codedName + "&citystatezip=" + codedCity
        )

        return self.getUrlContents(url, debug)

    def parseSearchSoupGetDetailsSoup(self, searchSoup, debug):
        """parseSearchSoupGetDetailsUrl.  Returns the url to get details for
        the first hit in the search.
        """

        # Get div tags for each hit
        L = searchSoup.find_all(
            "div",
            {"class": "card card-body shadow-form card-summary pt-3"}
        )

        try:
            bestHit = L[0]
        except IndexError:
            raise RuntimeError('No hits for this search.')

        urlStub = bestHit.attrs['data-detail-link']

        url = "https://www.truepeoplesearch.com/" + urlStub

        return self.getUrlContents(url, debug)

    def encode(self, string):
        """encode.  Remove spaces and commas and replaace with %20 junk.
        """

        return string.replace(" ", "%20").replace(",", "%2C")

    def getUrlContents(self, url, debug):
        """getHtmlSoup.  Turns url into a BeautifulSoup instance.
        """

        # The website is immune to proxies...
        # req = None
        # while req is None:
        #     try:
        #         req = self.proxyRequestUrl(url)
        #     except:
        #         print('Proxy %s failed with connection error.  Trying again.'
        #               % url)
        #         pass

        # It is also rate limited.
        sleep(5)
        req = requests.get(url)

        # TODO!! - Exception throwing for admin-ban (look for message in html
        # text)

        htmlText = req.content
        soup = BeautifulSoup(htmlText, 'html.parser')

        if debug:
            embed()

        return soup

    def proxyRequestUrl(self, url):
        proxy = next(self.proxies)
        return requests.get(url, proxies={'http': proxy, 'https': proxy})

    def parseSoupGetPhoneNumbers(self, detailsSoup):
        """parseSoupGetPhoneNumbers.  Given soup of details page, returns list
        of all phone numbers.

        Parameters
        ----------
        detailsSoup :
            detailsSoup - contents of "details" page for one query
        """

        # Get all div tags with "content-value" class
        contentCells = detailsSoup.find_all('div', {'class': 'content-value'})

        # Only some (maybe none) of these will contain phone numbers
        phoneNumbers = []

        for x in contentCells:
            # Phone numbers should be links ("a" tags) with the following settings.
            xPhoneNumCells = x.find_all(
                "a", {
                    "class": "link-to-more olnk",
                    "data-link-to-more": "phone"
                }
            )

            for cell in xPhoneNumCells:
                phoneNumbers.append(cell.text)

        return phoneNumbers

    @staticmethod
    def getProxies():
        """getProxies.  Taken from
        https://www.scrapehero.com/how-to-rotate-proxies-and-ip-addresses-using-python-3/
        """
        url = 'https://free-proxy-list.net/'
        # No need to proxy for this request.
        response = requests.get(url)
        parser = fromstring(response.text)
        proxies = set()
        for i in parser.xpath('//tbody/tr')[:10]:
            if i.xpath('.//td[7][contains(text(),"yes")]'):
                # Grabbing IP and corresponding PORT
                proxy = ":".join([i.xpath('.//td[1]/text()')
                                  [0], i.xpath('.//td[2]/text()')[0]])
                proxies.add(proxy)
        return proxies
