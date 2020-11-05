from analyze.derived_columns import addDerivedColumns
from bs4 import BeautifulSoup
from datetime import date
from functools import reduce
from pyquery import PyQuery as pq
import pandas as pd
import requests


class DenverCaseScraper:
    """DenverCaseScraper.  A class for scraping cases from denvercountycourt.org.

    Construct with necessary metadata and run .scrape(docketDf), where docketDf
    is the output of DenverDocketScraper.scrape.
    """

    # Static members
    outputColumns = [
        'case_number',
        'date',
        'room',
        'case_title',
        'type',
        'total_amount',
        'plaintiff',
        'defendant',
        'plaintiff_attorney',
        'defendant_attorney',
        'action_history',
        'scraped_on',
    ]

    def __init__(self, sessId, urlToken):
        """__init__.  Construct a DenverCaseScraper instance.

        Parameters
        ----------
        date : str
            date for the docket.  Used for constructing the URL.
        sessId : str
            PHPSESSID cookie for requesting as part of the active session.
        room : str
            room to scrape.  Used for constructing the URL
        urlToken : str
            token for constructing the case URL
        """
        self.scrapedOn = str(date.today())
        self.token = urlToken
        self.sessId = sessId
        self.soup = None

    def scrape(self, docketDf):
        """scrape.  Scrape every case number in docket_df.

        Parameters
        ----------
        docketDf : pandas.DataFrame
            docketDf - dataframe with case number, room, and date.
        """

        # Drop all cases with empty fields (they seem to all have vacated court
        # dates) and also de-dupe repeat rows by case.  This can happen for
        # multiple parties on a given case.
        df = (docketDf
              .dropna(subset=['case_number'])
              .drop_duplicates(subset=['case_number', 'date', 'room']))

        outputDf = pd.DataFrame(columns=DenverCaseScraper.outputColumns)

        print('Grabbing {} cases.'.format(len(df['case_number'])))
        for i, idx in enumerate(df.index):
            caseNum = df.loc[idx, 'case_number']
            date = df.loc[idx, 'date']
            room = df.loc[idx, 'room']

            print('Grabbing case number: ' + caseNum)
            try:
                row = self.scrapeSingleCase(caseNum, date, room)
            except:
                print('Scraping failed for case number %s.' % caseNum)
                row = ([caseNum, date, room, self.scrapedOn] +
                       [''] * (len(DenverCaseScraper.outputColumns) - 4))

            outputDf.loc[i] = row

        # Dropping non-FED cases for space savings and privacy of parties.
        outputDf = outputDf[outputDf['type'] == 'FED']

        return addDerivedColumns(outputDf)

    def scrapeSingleCase(self, caseNum, date, room):
        """scrapeSingleCase.  Scrape a single case number and return a list of
        the desired information.

        Parameters
        ----------
        caseNum : str
            case number to scrape
        """

        url = (
            'https://www.denvercountycourt.org/search/?casenumber=' + caseNum +
            '&date=' + date + '&room=' + room + '&token=' + self.token +
            '&searchtype=searchdocket'
        )
        response = requests.get(url, cookies={'PHPSESSID': self.sessId})
        self.soup = BeautifulSoup(response.content, 'html.parser')

        # Get FED/MONEY by parsing html

        # Table containing general facts about case
        statusTable = self.getTable(cl='status')

        # Get some basic facts
        caseTitle = self.findValInTable(statusTable, 'Case Title:')
        caseType = self.findValInTable(statusTable, 'Type:')
        totalAmt = self.findValInTable(statusTable, 'Total:')

        # Get Plaintiff(s) and defendant(s)
        # These results are kept in one table for each party.
        partyTables = self.getTables(cl='party')
        partyDf = self.glueTables(partyTables)

        # Separate into plaintiffs and defendants
        plaintiffDf = partyDf[partyDf['Party Type'] == 'PLAINTIFF']
        defendantDf = partyDf[partyDf['Party Type'] == 'DEFENDANT']
        plaintiff = self.collect(plaintiffDf['Name'])
        plaintiffAttorney = self.collect(plaintiffDf['Attorney Name'])
        defendant = self.collect(defendantDf['Name'], sep='|')
        defendantAttorney = self.collect(defendantDf['Attorney Name'])

        # Actions taken on case.
        actionTable = self.getTable(cl='actions')
        actionDf = (self.tableToDf(actionTable)
                    .sort_values('Act Date')
                    .fillna(''))

        actionDf['full_history'] = (
            actionDf['Act Date'] + '|' +
            actionDf['Description'] + '|' +
            actionDf['Status']
        )
        actionHistory = self.collect(actionDf['full_history'])

        return [
            caseNum,
            date,
            room,
            caseTitle,
            caseType,
            totalAmt,
            plaintiff,
            defendant,
            plaintiffAttorney,
            defendantAttorney,
            actionHistory,
            self.scrapedOn,
        ]

    def collect(self, series, sep=','):
        """collect.  Converts series to string with entries comma-separated

        Parameters
        ----------
        series : pandas.Series
            Data to collect.
        sep : str
            delimiter for the resulting string.  Comma by default.
        """

        vals = list(series.values)
        return reduce(lambda x, y: x + sep + ' ' + y, vals)

    def glueTables(self, tableList):
        """glueTables.  Union identical tables into a dataframe.

        Parameters
        ----------
        tableList : list[pandas.DataFrame]
            Tables to concatenate.  Must have compatible schema.
        """

        dfs = map(lambda table: self.tableToDf(table), tableList)

        return pd.concat(dfs).reset_index(drop=True)

    def tableToDf(self, table):
        """tableToDf.  Turn table into dataframe.

        Parameters
        ----------
        table : bs4.element.Tag
            Must be a table tag.
        """

        headers = self.getHeadersFromTable(table)
        df = pd.DataFrame(columns=headers)

        rows = table.findChildren(name='tr')

        for i, row in enumerate(rows[1:]):
            cells = [cell.text for cell in row.findChildren()]
            try:
                df.loc[i] = cells
            except ValueError:
                print('Failed to record row.  Continuing.')
                continue

        return df

    def getHeadersFromTable(self, table):
        """getHeadersFromTable.  Return text of each cell in first row of table.

        Parameters
        ----------
        table : bs4.element.Tag
            Must be a table tag.
        """

        rows = table.findChildren(name='tr')
        headers = list(map(lambda cell: cell.text, rows[0].findChildren()))

        return headers

    def getTable(self, cl):
        """getTable.

        Parameters
        ----------
        cl : str
            class for the table tag.  This will find the first instance.
        """
        return self.soup.findChild(name='table', attrs={'class': cl})

    def getTables(self, cl):
        """getTables.  Get list of tables of the given class

        Parameters
        ----------
        cl : str
            class for the table tag.  This will find the first instance.
        """
        return self.soup.findChildren(name='table', attrs={'class': cl})

    def findValInTable(self, tableOb, val):
        """findValInTable.  Find value of cell labeled by value of val, i.e.,
        the cell immediately to the right of the cell with value equal to val.
        raises RuntimeError if there is not exactly one.

        Parameters
        ----------
        tableOb : bs4.element.Tag
            table to scan
        val : str
            label to look for
        """

        rows = tableOb.findChildren(name='tr')
        hitValueList = reduce(
            lambda x, y: x + y,
            map(lambda row: self.findHitsInRow(row, val), rows)
        )

        if len(hitValueList) == 0:
            print('Did not find any hits for cell %s.' % val)
            return ''
        elif len(hitValueList) > 1:
            raise RuntimeError('Found too many hits for cell %s.' % val)
        else:
            return hitValueList[0]

    def findHitsInRow(self, trOb, val):
        """findHitsInRow.  Finds the value of any cell labeled by value of val
        and returns all values in a list.

        Parameters
        ----------
        trOb : list[bs4.element.Tag]
            A list of tr tags
        val : str
            label to look for.
        """

        cells = trOb.findChildren()

        hits = []
        for cell, nextCell in zip(cells[:-1], cells[1:]):
            if cell.text == val and nextCell.name == 'td':
                hits.append(nextCell.text)

        return hits
