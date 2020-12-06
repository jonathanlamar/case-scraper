from IPython import embed
from analyze.agg_tables import AggTables
from analyze.derived_columns import DERIVED_COLUMNS, numHearingsPerCase
from df2gspread import df2gspread as d2g
from dotenv import load_dotenv
from glob import glob
from oauth2client.service_account import ServiceAccountCredentials
import gspread
import numpy as np
import os
import pandas as pd

load_dotenv()


class SheetsIngest:
    """SheetsIngest.  A class for handling the ingest of data into google sheets.  """

    def __init__(self, serviceAccountConfigLoc):
        """__init__.  Create a SheetsIngest instance.

        Parameters
        ----------
        serviceAccountConfigLoc : str
            relative file path to the service account certificate json file.
        """

        self.serviceAccountConfigLoc = serviceAccountConfigLoc
        self.scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        self.credentials = ServiceAccountCredentials.from_json_keyfile_name(
            serviceAccountConfigLoc, self.scope)
        self.gc = gspread.authorize(self.credentials)

    def ingestNewBatchAndUpload(self, newlyScrapedCases, countySheetId):
        """ingestNewBatchAndUpload.  Performs ingest (i.e., processing and
        upload) of scraped cases

        Parameters
        ----------
        newlyScrapedCases : pandas.DataFrame
            output of case scraper
        countySheetId : str
            sheet id for the county
        """

        fullDf = self.ingestNewBatchToDf(newlyScrapedCases, countySheetId)
        self.uploadAggDfs(fullDf, countySheetId)
        self.uploadToSheets(fullDf, countySheetId, worksheetName='all_cases')

    def uploadAggDfs(self, fullDf, countySheetId):
        """uploadAggDfs.  Uploads weekly and monthly rollups of fullDf to sheets.

        Parameters
        ----------
        fullDf : pandas.DataFrame
            fullDf cases to be rolled up
        countySheetId : str
            ID for sheets target
        """
        aggTables = AggTables(fullDf)

        # Since we have fullDf, we can just overwrite here.
        self.uploadToSheets(aggTables.aggStatsWeekly(),
                            countySheetId, worksheetName='weekly_totals')
        self.uploadToSheets(aggTables.aggStatsMonthly(),
                            countySheetId, worksheetName='monthly_totals')

    def ingestNewBatchToDf(self, newlyScrapedCases, countySheetId):
        """ingestNewBatchToDf.  Performs ingest of scraped cases into a
        dataframe.

        Parameters
        ----------
        newlyScrapedCases : pandas.DataFrame
            output of case scraper
        countySheetId : str
            sheet id for the county
        """

        oldCases = self.downloadSheetToDf(
            countySheetId, worksheetName='all_cases')
        # Lowercase columns for easy comparisons
        oldCases.columns = oldCases.columns.str.lower()
        newlyScrapedCases.columns = newlyScrapedCases.columns.str.lower()

        if oldCases.shape[0] > 0:
            fullDf = self.joinWithOldCasesAndNotes(newlyScrapedCases, oldCases)
        else:
            fullDf = newlyScrapedCases

        return fullDf.sort_values('date')

    def downloadSheetToDf(self, countySheetId, worksheetName=None):
        """downloadSheetToDf.  Downloads a worksheet to a pandas dataframe

        Parameters
        ----------
        countySheetId : str
            sheet id for the county
        worksheetName : str
            page name for the upload
        """

        googleSheet = self.gc.open_by_key(countySheetId)
        worksheet = googleSheet.worksheet(worksheetName)

        data = worksheet.get_all_values()  # Empty for blank worksheet
        try:
            headers = data.pop(0)
        except IndexError:
            headers = []

        df = pd.DataFrame(data, columns=headers)
        if df.shape[0] > 0:
            # Cast timestamp date column to date strings.
            df['date'] = pd.to_datetime(df['date']).apply(
                lambda x: str(x.date()))
            # Years are read as floats maybe?
            df['year'] = df['year'].astype(str)
            # Boolean flag columns are returned as strings.
            df['writ_of_restitution'] = df['writ_of_restitution'].apply(eval)
            df['evicted_flag'] = df['evicted_flag'].apply(eval)

        return df

    def joinWithOldCasesAndNotes(self, newlyScrapedCases, oldCases):
        """joinWithOldCasesAndNotes.  Joins to old cases, dedupes based on case
        date and scrape date, and removes bad rows.

        Parameters
        ----------
        newlyScrapedCases : pandas.DataFrame
            dataframe to process
        oldCases : pandas.DataFrame
            dataframe of previously existing cases downloaded from sheets.
        """

        newlyScrapedCases = newlyScrapedCases.dropna(subset=['case_number'])

        # TODO: There should be no need for this.  Remove once sheets dataset is
        # clean.
        if oldCases[oldCases['case_number'] == 'nan'].shape[0] > 0:
            print(
                'WARNING: Found nans in existing google sheet. Please remove.')
            oldCases = oldCases[oldCases['case_number'] != 'nan']

        notesColumns = [col for col in oldCases.columns
                        if col not in newlyScrapedCases.columns]

        oldNotes = oldCases[['case_number'] + notesColumns]
        oldCases = oldCases.drop(notesColumns, axis=1)

        return (self.joinAndDedupe(newlyScrapedCases, oldCases)
                .merge(oldNotes,
                       how='left',
                       left_on='case_number',
                       right_on='case_number')
                .dropna(subset=['case_number']))

    def joinAndDedupe(self, newlyScrapedCases, oldCases):
        """joinAndDedupe.  Returns the union of newlyScrapedCases and oldCases,
        where duplicate case numbers are filtered to keep only the most recent
        when ordered by date and scraped_on.

        Parameters
        ----------
        newlyScrapedCases : pandas.DataFrame
            dataframe to process
        oldCases : pandas.DataFrame
            dataframe of previously existing cases downloaded from sheets.
        """

        dataWithDupes = (pd.concat([oldCases, newlyScrapedCases])
                         .drop('num_hearings', axis=1))

        # Refresh this column with the new data
        numHearings = numHearingsPerCase(dataWithDupes)

        return (dataWithDupes
                .sort_values(['date', 'scraped_on'])
                .groupby('case_number').last()
                .merge(
                    numHearings,
                    left_on=['case_number', 'date'],
                    right_on=['case_number', 'date']))

    def uploadToSheets(self, toUploadData, countySheetId, worksheetName=None):
        """uploadToSheets.  Uploads a dataframe.

        Parameters
        ----------
        newlyScrapedCases : pandas.DataFrame
            dataframe to upload.  Will not upload index.
        countySheetId : str
            sheet id of target
        worksheetName : str
            page name for the upload
        """

        d2g.upload(
            toUploadData,
            countySheetId,
            wks_name=worksheetName,
            credentials=self.credentials,
            row_names=False)
