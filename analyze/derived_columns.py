from IPython import embed
from datetime import date
from functools import reduce
import numpy as np
import pandas as pd


DERIVED_COLUMNS = [
    'year',
    'month',
    'week',
    'writ_of_restitution',
    'evicted_flag',
    'num_hearings',
]


def addDerivedColumns(df):
    """addDerivedColumns.  Adds flags and other helpful columns.

    Parameters
    ----------
    df : pandas.DataFrame
        All cases, including non-eviction cases.
    """

    # Infer scraped_on if not present
    if 'scraped_on' not in df.columns:
        df['scraped_on'] = str(date.today())

    # Easier filtering
    df['year'] = pd.to_datetime(df['date']).apply(
        lambda date: date.year).astype(str)
    df['month'] = pd.to_datetime(df['date']).apply(
        lambda date: date.month).astype(str)
    df['week'] = pd.to_datetime(df['date']).apply(
        lambda date: date.week).astype(str)

    # Add derived columns
    df['writ_of_restitution'] = (df['action_history']
                                 .apply(writOfRestitutionFlag))
    df['evicted_flag'] = df['action_history'].apply(evictedFlag)

    # TODO: This might not be good to have here universally.
    numHearings = numHearingsPerCase(df)

    if 'num_hearings' in df.columns:
        df = df.drop('num_hearings', axis=1)

    return df.merge(
        numHearings,
        left_on=['case_number', 'date'],
        right_on=['case_number', 'date']
    )


def numHearingsPerCase(casesDf):
    # Groupby on empty dataframe breaks the schema. Ugh
    if casesDf.shape[0] == 0:
        return pd.DataFrame(columns=['case_number', 'date', 'num_hearings'])

    return (casesDf.groupby(['case_number', 'date'])
            .agg(num_hearings=('case_number', np.size))
            .reset_index())


def writOfRestitutionFlag(actionHistory):
    """writOfRestitutionFlag.  Simply looking for WRITOFRESTITUTION in the action
    history.

    Parameters
    ----------
    actionHistory : str
        a value from the action_history column
    """

    df = getHistoryDf(actionHistory)

    return any(df['action'] == 'WRITOFRESTITUTION')


def evictedFlag(actionHistory):
    """evictedFlag.  Flag true if the case had a writ of restitution, but was not
    thrown out.

    Parameters
    ----------
    actionHistory : str
        a value from the action_history column
    """

    df = getHistoryDf(actionHistory)

    # No writ means no eviction ordered.
    if not any(df['action'] == 'WRITOFRESTITUTION'):
        return False

    # Otherwise return true, unless the case was dismissed later.

    latestWrit = df.loc[df['action'] == 'WRITOFRESTITUTION', 'timestamp'].max()

    subsequentActions = df.loc[df['timestamp'] >= latestWrit, 'action']

    # These outcomes indicate the case was thrown out.  We take this as a sign
    # the parties settled or that the case was thrown out by the judge for some
    # other reason.
    return (
        not any(subsequentActions == 'DISMISSEDWITHPREJUDICE') and
        not any(subsequentActions == 'DISMISSEDWITHOUTPREJUDICE')
    )


def getHistoryDf(actionHistory):
    """getHistoryDf.  Read action history string into a dataframe.

    Parameters
    ----------
    actionHistory : str
        a value from the action_history column
    """

    df = pd.DataFrame(
        [row.split('|') for row in actionHistory.split(',')],
        columns=['timestamp', 'action', 'status']
    )

    df['action'] = df['action'].apply(normalizeStr)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp')

    return df


def normalizeStr(string):
    """normalizeStr.  remove all non-alphanumeric characters and convert to upper
    case.

    Parameters
    ----------
    string : str
        a string to normalize
    """

    return ''.join([c.upper() for c in string if c.isalnum()])
