from analyze.derived_columns import addDerivedColumns
from IPython import embed
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd

DENVER_DATA = {
    'sheet_id': '1YiaZerWNqjkLYvo7CvO938CeVkjGFzWMMiRrDH84lxo',
}


class Backfill:
    """Backfill.  Consolidate records from V1 of the denver google sheet.  """

    # Tabs titles to avoid when backfilling.
    IGNORE_TABS = [
        'pine creek',
        'weekly_totals',
        'monthly_totals',
        'routes',
        'main evictors',
        'all cases',
        'lit drop',
    ]

    def __init__(self, serviceAccountConfigLoc):
        """__init__.  Create a Backfill instance.

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

    def pullAllTabsAsOneDataframe(self, countySheetId):
        """pullAllTabsAsOneDataframe.  Download all the tabs, concatenate as one
        dataframe, and dedupe.

        Parameters
        ----------
        countySheetId : str
            google sheets ID of the sheet
        """

        googleSheet = self.gc.open_by_key(countySheetId)
        worksheets = []
        for ws in googleSheet.worksheets():
            if all([ws.title.lower().find(colName) == -1
                    for colName in Backfill.IGNORE_TABS]):
                worksheets.append(ws)

        outDf = pd.DataFrame()
        for worksheet in worksheets:
            # x = input('Processing %s.  Skip? [yN]: ' % worksheet.title)
            # if len(x) > 0 and x[0].lower() == 'y':
            #     print('Skipping %s.' % worksheet.title)
            #     continue

            data = worksheet.get_all_values()  # Empty for blank worksheet

            try:
                headers = data.pop(0)
            except IndexError:
                headers = []

            df = pd.DataFrame(data, columns=headers)

            if df.shape[0] == 0:
                print('No rows to backfill.  Skipping %s.' % worksheet.title)
                continue

            if ('type' not in df.columns
                    or 'case_number' not in df.columns
                    or 'date' not in df.columns):
                print('Missing necessary columns.  Skipping %s.'
                      % worksheet.title)
                continue

            try:
                df = df[df['type'] == 'FED']
                df = df[(df['case_number'].isnull() == False)
                        & (df['case_number'] != '')]
                df['date'] = self.fixDates(df)

                if 'case number' in df.columns:
                    df = df.drop('case number', axis=1)
                if 'party_disposition' in df.columns:
                    df = df.drop('party_disposition', axis=1)
                if 'initial_disposition' in df.columns:
                    df = df.drop('initial_disposition', axis=1)

                if any(df['date'] == '########'):
                    df = df[df['date'] != '########']

                outDf = self.concatDedupe(outDf, df)
            except:
                print('Something went wrong.  Skipping %s.' % worksheet.title)

        return addDerivedColumns(outDf)

    def fixDates(self, df):
        return (pd.to_datetime(df['date'], infer_datetime_format=True)
                .apply(lambda date: str(date.date())))

    def concatDedupe(self, oldDf, newDf):
        dataWithDupes = pd.concat([oldDf, newDf])

        return (dataWithDupes
                .sort_values('date')
                .groupby('case_number').last()
                .reset_index()
                .fillna(''))  # Fill nans for upload


if __name__ == '__main__':
    backfill = Backfill(serviceAccountConfigLoc='data/service_account.json')
    df = backfill.pullAllTabsAsOneDataframe(
        '1YiaZerWNqjkLYvo7CvO938CeVkjGFzWMMiRrDH84lxo')
