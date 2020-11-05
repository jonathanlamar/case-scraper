from glob import glob
from scrapers.phone_number_scraper import PhoneNumberScraper
import pandas as pd

################################################################################
# WARNING: This does not work.  The server is rate limited, and detects proxy
# cycling.  The results are also not high enough quality to bother fixing those
# issues.
################################################################################

counties = [
    'Adams',
    'Arapahoe',
    'Denver',
    'Douglas',
    'Jefferson',
]

# TODO: These were examples used for testing.  This would be set through .env
csvs = {
    'Adams': '~/Downloads/Copy of FED Cases - Adams County.csv',
    'Arapahoe': '~/Downloads/Copy of FED Cases - Arapahoe County.csv',
    'Denver': '~/Downloads/Test-Grid view.csv',
    'Douglas': '~/Downloads/Copy of FED Cases - Douglas County.csv',
    'Jefferson': '~/Downloads/Copy of FED Cases - Jefferson County.csv',
}

# TODO: Map to a list of cities and make requests for each.
cities = {
    'Adams': 'Denver, CO',  # OrgetNumbersForPeople
    'Arapahoe': 'Aurora, CO',  # Or Littleton, or Centennial
    'Denver': 'Denver, CO',
    'Douglas': 'Highlands Ranch, CO',  # Parker, Castle Rock
    'Jefferson': 'Lakewood, CO',  # Golden, Arvada
}

for county in counties:
    print('Processing %s.' % county)

    docketDf = pd.read_csv(csvs[county]).reset_index()

    if county == 'Denver':
        caseCol = 'case_number'
        df = docketDf.loc[
            docketDf['type'] == 'FED',
            [caseCol, 'defendant']]
    else:
        caseCol = 'Case #'
        df = docketDf[[caseCol,  'Name']]

    df['City'] = cities[county]

    # Use name and city columns that phone scraper expects
    df.columns = [caseCol, 'Name', 'City']

    scraper = PhoneNumberScraper(df)
    df = scraper.getPhoneNumbers(debug=False)

    outDf = docketDf.merge(df, left_on=caseCol, right_on=caseCol, how='left')

    outDf.to_csv('output_' + csvs[county])
