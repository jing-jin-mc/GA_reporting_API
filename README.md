# GA_reporting_API
Use google analytics reporting api to fetch data from GA
* gaData.py
  - Have functions for connecting to GA and fetching data avoiding sampling.
    - To avoid sampling in each API request, something called nextPageToken has been used to enable more than 10,000 results in each API request.
    - In this code, by default, An API request will be made day by day no matter how large the date range you are giving, which means we sent request for data at a daily level.
    - If the data is somehow sampled, a samplesReadCounts and samplingSpaceSizes will be printed out so you know sampling happens.
  - Functions:
    - initialize_analyticsreporting(CLIENT_SECRETS_PATH)
    - return_ga_data(analytics,
                       start_date,
                       end_date,
                       view_id,
                       metrics,
                       dimensions,
                       segments = [],
                       split_dates = True,
                       group_by = [],
                       SLEEP_TIME = 2
                      )
    - get_and_save_data(path,
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
                        )
  - save_df_to_csv(df,
                  path,
                  filename
                  )

* Get_ClientID_List.ipynb
  - A Jupyter Notebook template for how to use gaData.py to get data from GA
* key
  - a folder you should put your own client credentials
