from IPython import embed
from analyze.agg_tables import AggTables
from analyze.derived_columns import addDerivedColumns, DERIVED_COLUMNS
from dotenv import load_dotenv
from glob import glob
from ingest.sheets_ingest import SheetsIngest
from itertools import product
from scrapers.denver_case_scraper import DenverCaseScraper
from scrapers.denver_dockets import DenverDocketScraper
import os
import pandas as pd

DENVER_DATA = {
    'sheet_id': '1eZq7IVnLhzGGkRsVHLlpr3U_e7ul_F11tXlUJ6W7yHo',
}

filenames = glob('data/*.csv')
sheetsIngest = SheetsIngest(serviceAccountConfigLoc=os.getenv('GOOGLE_TOKEN'))

for filename in filenames:
    x = input('Processing %s... Skip? [yN]: ' % filename)
    if len(x) > 0 and x[0].lower() == 'y':
        continue

    file = (pd.read_csv(filename)
            .dropna(subset=['case_number']).reset_index(drop=True)
            .fillna(''))
    file = file[file['type'] == 'FED']

    if 'party_disposition' in file.columns:
        file = file.drop('party_disposition', axis=1)

    coreCols = file[DenverCaseScraper.outputColumns].copy()
    noteCols = file.drop(DenverCaseScraper.outputColumns + DERIVED_COLUMNS,
                         # drop all if present, ignore otherwise
                         errors='ignore',
                         axis=1)

    # Just recompute the derived columns and make sure the notes are at the end.
    toIngestDf = addDerivedColumns(coreCols).join(noteCols)

    sheetsIngest.ingestNewBatchAndUpload(
        newlyScrapedCases=toIngestDf[toIngestDf['type'] == 'FED'],
        countySheetId=DENVER_DATA['sheet_id']
    )
