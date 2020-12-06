from bs4 import BeautifulSoup
import pandas as pd
import requests


class DenverDocketScraper:
    """DenverDocketScraper.

    A class for scraping dockets from denvercountycourt.org
    """

    outputColumns = [
        'case_number',
        'date',
        'room',
    ]

    def __init__(self, date=None, sessId=None, room=None, urlToken=None):
        """__init__.  Construct a DenverDocketScraper instance.

        Parameters
        ----------
        date : str
            date for the docket
        sessId : str
            PHPSESSID cookie for requesting as part of the active session.
        room : str
            room to query
        urlToken : str
            token
        """
        self.date = date
        self.sessId = sessId
        self.room = room
        self.urlToken = urlToken

    def parse(self, trOb, classname):
        """parse.  Finds element of row with given class and returns text.

        Parameters
        ----------
        trOb : bs4.element.Tag
            An element of tr tag representing a row of the docket
        classname : str
            class name to look for in children
        """
        try:
            obText = trOb.findChild(attrs={'class': classname}).getText()
        except:
            obText = None
        return obText

    def postProcess(self, df):
        """postProcess.  Rename and drop columns from the raw scraped data.

        Parameters
        ----------
        df : pandas.DataFrame
            raw scraped output from the docket page.
        """
        # Fix columns for compatibility with CaseScraper
        df['case_number'] = df['case_no']

        # Columns which are redundant or totally blank.
        dropCols = [
            'case_no',
            'defendant',
            'center',
            'disposition',
            'courtroom',
            'date',
        ]

        cleanedDf = (df
                     # Remove first row because the corresponding tr cell is
                     # empty.  FIXME: That should be handled in situ.
                     .iloc[1:]
                     .drop(dropCols, axis=1)
                     .dropna(subset=['case_number'])
                     .reset_index(drop=True))
        cleanedDf['date'] = self.date
        cleanedDf['room'] = self.room

        return cleanedDf

    def scrape(self):
        """scrape.  Scrape all cases from the docket.  """

        # Check to make sure all variables have been set
        requiredParams = [self.urlToken, self.sessId, self.date, self.room]
        if any([x is None for x in requiredParams]):
            raise Exception('Error: Need to set all docket parameters first.')

        # Get the html and parse
        url = (
            'https://www.denvercountycourt.org/courtroom-calendar/' +
            '?searchtype=searchdocket' +
            '&date=' + self.date +
            '&room=' + self.room +
            '&token=' + self.urlToken
        )

        response = requests.get(url, cookies={'PHPSESSID': self.sessId})
        soup = BeautifulSoup(response.content, 'html.parser')

        # Get all tr opbjects
        trObs = soup.find_all(name='tr')

        # Get all td objects with a given class and parse text.
        # When this is blank, the court date has probably been vacated,
        # however they show up in the docket anyway.
        keys = [
            'case_no',
            'center',
            'courtroom',
            'disposition',

            # Point of confusion.  This is called defendant, but could be
            # either party.
            'defendant',

            'date'
        ]

        textDict = {
            key: list(map(lambda x: self.parse(x, key), trObs)) for key in keys
        }

        docketDf = pd.DataFrame(textDict)
        return self.postProcess(docketDf)
