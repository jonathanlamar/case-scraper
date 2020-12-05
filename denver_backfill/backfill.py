from analyze.agg_tables import AggTables
from analyze.derived_columns import addDerivedColumns, DERIVED_COLUMNS
from dotenv import load_dotenv
from ingest.sheets_ingest import SheetsIngest
from glob import glob
from itertools import product
from scrapers.case_scraper import DenverCaseScraper
from scrapers.docket_scraper import DenverDocketScraper
from IPython import embed
import os
import pandas as pd

DENVER_DATA = {
    'sheet_id': '1-7maDH9l0Gg2EZ07aJNq_jeW1XujxDByLNruERT184c',
}

filenames = glob('data/*.csv')
sheetsIngest = SheetsIngest(serviceAccountConfigLoc=os.getenv('GOOGLE_TOKEN'))

for filename in filenames:
    print('Processing %s...' % filename)
    file = pd.read_csv(filename).dropna(
        subset=['case_number']).reset_index(drop=True)
    file = file[file['type'] == 'FED']

    if 'party_disposition' in file.columns:
        file = file.drop('party_disposition', axis=1)

    if (len([col for col in DERIVED_COLUMNS if col in file.columns])
            < len(DERIVED_COLUMNS)):
        print('Missing derived columns.  Adding back...')
        file = addDerivedColumns(file)

    sheetsIngest.ingestNewBatchAndUpload(
        newlyScrapedCases=file[file['type'] == 'FED'],
        countySheetId=DENVER_DATA['sheet_id']
    )
    input('Press Enter to continue...')
