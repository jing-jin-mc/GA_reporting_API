# GA_reporting_API
Use google analytics reporting api to fetch data from GA
* gaData.py
- Have functions for connecting to GA and fetching data
- Functions:
  initialize_analyticsreporting(CLIENT_SECRETS_PATH)
  return_ga_data(analytics,
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
  get_and_save_data(path,
                    filesize,
                    analytics,
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
* Get_ClientID_List.ipynb
- Example Temple for how to get data from GA by using Jupyter Notebook
