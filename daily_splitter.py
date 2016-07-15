#!/usr/bin/env python
#-*- encoding: utf-8 -*-

import bigquery
import luigi
import luigi_bigquery
import os

from datetime import datetime as dt, timedelta
from sqlalchemy import create_engine


this_name, ext = os.path.splitext( os.path.basename(__file__) )


class DailySplitter( luigi_bigquery.QueryTable ):

    rdataset = luigi.Parameter()
    rtable   = luigi.Parameter()
    prefix   = luigi.Parameter()
    sdate    = luigi.DateParameter()
    edate    = luigi.DateParameter()

    source   = 'query/candles_by_date.sql'

    write_disposition  = bigquery.JOB_WRITE_EMPTY
    create_disposition = bigquery.JOB_CREATE_IF_NEEDED

    def dataset( self ):
        return self.rdataset

    def table( self ):
        return self.edate.strftime( '{0}_%Y%m%d' ).format( self.prefix )



if __name__ == '__main__':
    luigi.run()
