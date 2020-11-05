from datetime import timedelta
import numpy as np
import pandas as pd
from IPython import embed


class AggTables:
    def __init__(self, processedEvictionDf):
        """__init__.

        Parameters
        ----------
        processedEvictionDf : pandas.DataFrame
            Database of evictions which has been processed and deduped.
        """
        self.evictionDf = processedEvictionDf.copy()
        self.evictionDf['date'] = pd.to_datetime(processedEvictionDf['date'])
        self.evictionDf = self.evictionDf.sort_values('date')

    def aggStatsMonthly(self):
        """aggStatsMonthly.  Group by year and month and compute basic stats.  """

        # FIXME: This results in the months in alphabetical order.  They should
        # be in chronological order.
        aggDf = (self.evictionDf
                 .groupby(['year', 'month'])
                 .agg(
                     num_fed_hearings=('case_number', np.size),
                     num_writ_restitution=('writ_of_restitution', np.sum),
                     num_evictions=('evicted_flag', np.sum)
                 ))

        return self.addDerivedColumns(aggDf)

    def aggStatsWeekly(self):
        """aggStatsMonthly.  Group by year and month and compute basic stats.  """

        aggDf = (self.evictionDf
                 .groupby(['year', 'week'])
                 .agg(
                     week_start=('date', np.min),
                     num_fed_hearings=('case_number', np.size),
                     num_writ_restitution=('writ_of_restitution', np.sum),
                     num_evictions=('evicted_flag', np.sum)
                 ))

        # Correct date so all are Sunday
        aggDf['week_start'] = (
            aggDf['week_start']
            .apply(lambda date: self.getStartOfWeek(date))
        )

        aggDf = (aggDf
                 .reset_index()
                 .drop(['week', 'year'], axis=1)
                 .set_index('week_start'))

        return self.addDerivedColumns(aggDf)

    def getStartOfWeek(self, date):
        dateAsDateTime = pd.to_datetime(date)
        dateDiff = dateAsDateTime - timedelta(days=dateAsDateTime.weekday())

        return str(dateDiff.date())

    def addDerivedColumns(self, aggDf):
        """addDerivedColumns.  Adds summary stats to aggregate dataframe.

        Parameters
        ----------
        aggDf : pandas.DataFrame
            Weekly or Monthly aggs from self.evictionDf
        """
        modifiedAggDf = aggDf.reset_index()
        modifiedAggDf['eviction_rate'] = (
            modifiedAggDf['num_evictions'] / modifiedAggDf['num_fed_hearings'])
        modifiedAggDf['judgement_rate'] = (
            modifiedAggDf['num_writ_restitution'] /
            modifiedAggDf['num_fed_hearings'])
        modifiedAggDf['mediation_rate'] = (
            (modifiedAggDf['num_writ_restitution']
             - modifiedAggDf['num_evictions']) /
            modifiedAggDf['num_fed_hearings'])

        return modifiedAggDf
