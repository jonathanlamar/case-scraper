from analyze.agg_tables import AggTables
from analyze.derived_columns import addDerivedColumns
from dotenv import load_dotenv
from glob import glob
from ingest.sheets_ingest import SheetsIngest
from itertools import product
from scrapers.case_scraper import DenverCaseScraper
from scrapers.docket_scraper import DenverDocketScraper
import os
import pandas as pd

load_dotenv()

# Set parameters in the .env in this directory (ignored by git.  DENVER_SESS_ID
# is the PHPSESSID cookie, which can be found by looking around in the developer
# tools menu of most web browsers (chrome, firefox, etc.).  URL_TOKEN can be
# found in the URL of a case page, listed as '?token=<XXXXXX>'.
firstDate = os.getenv('FIRST_DATE')
lastDate = os.getenv('LAST_DATE')
sessId = os.getenv('DENVER_SESS_ID')
urlToken = os.getenv('DENVER_URL_TOKEN')
outputName = os.getenv('DENVER_OUTPUT_FILENAME')

# Courtrooms.
rooms = [
    '104',
    '170',
    '186',
    '175'
]

DENVER_DATA = {
    'sheet_id': '1-7maDH9l0Gg2EZ07aJNq_jeW1XujxDByLNruERT184c',
    # TODO: This is a test sheet to avoid overwriting the one we share.
    'weekly_sheet_id': '1kCKuu5-JgdNonTGRY0cqlHV0GuBZ83coaSJaZUf1LYM',
    'monthly_sheet_id': '1yn7yPbzU_z0d4aCnewx5rd24pon_ieuWtMNB4mKSyWE',
}


dates = [str(_.date()) for _ in pd.date_range(firstDate, lastDate)]
allCasesDf = pd.DataFrame(columns=DenverCaseScraper.outputColumns)

for date, room in product(dates, rooms):
    print('Grabbing cases on %s in room %s.' % (date, room))

    docketScraper = DenverDocketScraper(date, sessId, room, urlToken)
    docketDf = docketScraper.scrape()

    if docketDf.shape[0] == 0:
        print('No cases.')
        continue

    # CaseScraper can now scrape all dockets at once, but that takes forever,
    # so we do one docket at a time.
    caseScraper = DenverCaseScraper(sessId, urlToken)
    casesDf = caseScraper.scrape(docketDf)

    print('Saving csv backup at %s.' % outputName)
    casesDf.to_csv('%s__%s__%s.csv' % (outputName, date, room), index=False)

    allCasesDf = pd.concat([allCasesDf, casesDf])

# pandas.concat can result in duplicate index values.  This causes d2g to error.
ingestDf = allCasesDf.reset_index(drop=True)

print('Ingesting FED cases to google sheets')
sheetsIngest = SheetsIngest(serviceAccountConfigLoc=os.getenv('GOOGLE_TOKEN'))
sheetsIngest.ingestNewBatchAndUpload(
    newlyScrapedCases=ingestDf,
    countySheetId=DENVER_DATA['sheet_id']
)
