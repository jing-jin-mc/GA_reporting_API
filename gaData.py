
######### Libraries ################
import argparse
from googleapiclient.discovery import build
import httplib2
from oauth2client import client
from oauth2client import file
from oauth2client import tools
import pandas as pd
import time
import os
import errno
from datetime import datetime, timedelta
from time import sleep


#################################################
SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
DISCOVERY_URI = ('https://analyticsreporting.googleapis.com/$discovery/rest')
######### Function: connect to the GA ################
######### Function will be called outside ######
def initialize_analyticsreporting(CLIENT_SECRETS_PATH):
  """Initializes the analyticsreporting service object.

  Returns:
    analytics an authorized analyticsreporting service object.
  """
  # Parse command-line arguments.
  parser = argparse.ArgumentParser(
      formatter_class=argparse.RawDescriptionHelpFormatter,
      parents=[tools.argparser])
  flags = parser.parse_args([])

  # Set up a Flow object to be used if we need to authenticate.
  flow = client.flow_from_clientsecrets(
      CLIENT_SECRETS_PATH, scope=SCOPES,
      message=tools.message_if_missing(CLIENT_SECRETS_PATH))

  # Prepare credentials, and authorize HTTP object with them.
  # If the credentials don't exist or are invalid run through the native client
  # flow. The Storage object will ensure that if successful the good
  # credentials will get written back to a file.
  storage = file.Storage('analyticsreporting.dat')
  credentials = storage.get()
  if credentials is None or credentials.invalid:
      credentials = tools.run_flow(flow, storage, flags)

  http = credentials.authorize(http=httplib2.Http())

  # Build the service object.
  analytics = build('analytics', 'v4', http=http, discoveryServiceUrl=DISCOVERY_URI,cache_discovery=False)

  return analytics

######### Functions: configure the report ################
######## Will be called inside function: get_ga_data() ######
def get_report(analytics, nextPageToken, start_date, end_date, view_id, metrics, dimensions, segments):
  # Use the Analytics Service Object to query the Analytics Reporting API V4.
  return analytics.reports().batchGet(
      body={
        "reportRequests":
  [
    {
        "viewId": view_id,
        "dateRanges": [{"startDate": start_date, "endDate": end_date}],
        "metrics": metrics,
        "dimensions":dimensions,
        "pageToken": nextPageToken,
        "pageSize" : 10000,
        "segments":segments,
        "includeEmptyRows": "true",
        "samplingLevel": "DEFAULT",
        #"samplingLevel": "LARGE"
    }
  ]
      }
  ).execute()

######### Functions: transform the raw data to dataframe ################
#### Also print sample level if sampling happens #############
##### Will be called inside of function: get_ga_data()#########
def print_response(response):
  list = []
  # get report data
  for report in response.get('reports', []):
    # set column headers
    columnHeader = report.get('columnHeader', {})
    dimensionHeaders = columnHeader.get('dimensions', [])
    metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
    rows = report.get('data', {}).get('rows', [])

    samplesReadCounts = report.get('data', {}).get('samplesReadCounts', [])
    if samplesReadCounts != []:
        print ("Data has been sampled!")
        print (samplesReadCounts)
    samplingSpaceSizes = report.get('data', {}).get('samplingSpaceSizes', [])
    if samplingSpaceSizes != []:
        print (samplingSpaceSizes)

    for row in rows:
        # create dict for each row
        dict = {}
        dimensions = row.get('dimensions', [])
        dateRangeValues = row.get('metrics', [])

        # fill dict with dimension header (key) and dimension value (value)
        for header, dimension in zip(dimensionHeaders, dimensions):
          dict[header] = dimension

        # fill dict with metric header (key) and metric value (value)
        for i, values in enumerate(dateRangeValues):
          for metric, value in zip(metricHeaders, values.get('values')):
            #set int as int, float a float
            if ',' in value or ',' in value:
              dict[metric.get('name')] = float(value)
            else:
              dict[metric.get('name')] = float(value)

        list.append(dict)

    df = pd.DataFrame(list)
    return df

######### Functions: split one query into several querys to get one day's unsampled data (more than 10,000 rows)  ################
###### Will be called inside function return_ga_data()#############
def get_ga_data(analytics,
                start_date,
                end_date,
                view_id,
                metrics,
                dimensions,
                segments,
                SLEEP_TIME):
    ga =get_report(analytics,
                   "",
                   start_date,
                   end_date,
                   view_id,
                   metrics,
                   dimensions,
                   segments
                  )
    df_total = print_response(ga)
    token = ga.get('reports', [])[0].get('nextPageToken')
    while token is not None:
        ga = get_report(analytics,
                        token,
                        start_date,
                        end_date,
                        view_id,
                        metrics,
                        dimensions,
                        segments)
        df = print_response(ga)
        df_total = df_total.append(df)
        token = ga.get('reports', [])[0].get('nextPageToken')
        sleep(SLEEP_TIME)

    return df_total

######### Functions: fetch data day by day  ################
####### Function will be called outside ##############
def return_ga_data(analytics,
                   start_date,
                   end_date,
                   view_id,
                   metrics,
                   dimensions,
                   segments = [],
                   split_dates = True,
                   group_by = [],
                   SLEEP_TIME = 2
                  ):
    if split_dates == False:
        return get_ga_data(analytics,
                           start_date,
                           end_date,
                           view_id,
                           metrics,
                           dimensions,
                           segments,
                           SLEEP_TIME
                          )
    else:
        start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
        delta = end_date - start_date         # timedelta
        dates = []

        for i in range(delta.days + 1):
            dates.append(start_date + timedelta(days=i))

        df_total = pd.DataFrame()
        for date in dates:
            date = str(date)
            df_total = df_total.append(get_ga_data(analytics,
                                                   date,
                                                   date,
                                                   view_id,
                                                   metrics,
                                                   dimensions,
                                                   segments,
                                                   SLEEP_TIME
                                                  )
                                      )
            sleep(SLEEP_TIME)

        if len(group_by) != 0:
            if df_total.empty:
                return df_total
            else:
                df_total = df_total.groupby(group_by,
                                            as_index=False
                                            ).sum()
        return df_total

######### Functions: fetch data day by day and save data by filesize (number of days) ################
####### Function will be called outside ##############
def get_and_save_data(path,
                      filesize,
                      analytics,
                      start_date,
                      end_date,
                      view_id,
                      metrics,
                      dimensions,
                      segments,
                      split_dates = True,
                      group_by = [],
                      SLEEP_TIME = 5
                     ):
    ###############
    start_date = datetime.strptime(start_date,
                                   '%Y-%m-%d'
                                  ).date()
    end_date = datetime.strptime(end_date,
                                 '%Y-%m-%d'
                                ).date()
    delta = end_date - start_date
    dates = []
    dates_pair = []
    for i in range(delta.days+1):
        dates.append(start_date + timedelta(days=i))

    num_files = int(len(dates)/filesize)
    for j in range(num_files):
        start = dates[j*filesize]
        end = dates[j*filesize+filesize-1]
        dates_pair.append((start,end))
    if dates_pair[-1][1] < dates[-1]:
        dates_pair.append((dates_pair[-1][1]+timedelta(days=1),dates[-1]))
    #######################
    df_total = pd.DataFrame()
    for k in range(len(dates_pair)):
        filename = str(dates_pair[k][0])+"_"+str(dates_pair[k][1])+".csv"
        df = return_ga_data(analytics,
                            str(dates_pair[k][0]),
                            str(dates_pair[k][1]),
                            view_id, metrics,
                            dimensions,
                            segments,
                            split_dates,
                            group_by,
                            SLEEP_TIME
                           )
        save_df_to_csv(df,
                       path,
                       filename
                      )
        df_total = df_total.append(df)
    if len(group_by) != 0:
        if df_total.empty:
            return df_total
        else:
            df_total = df_total.groupby(group_by,
                                        as_index=False
                                       ).sum()
    return df_total

######### Functions: save data to a csv file  ################
####### Function will be called outside ##############
def save_df_to_csv(df, path, filename):
    file_loc = path + '/' + filename
    if not os.path.exists(os.path.dirname(file_loc)):
        try:
            os.makedirs(os.path.dirname(file_loc))
        except OSError as exc: # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise
    return df.to_csv(path_or_buf = file_loc, index= False )
