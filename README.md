# PatentsView downloader
Download data through the patent endpoint of the PatentsView API by date

## Example
Download data for January 2019 and write to csv files.
```python
import pandas as pd

fields = ['patent_number', 
          'patent_abstract',
          'patent_date',
          'assignee_id', 
          'assignee_country', 
          'inventor_id', 
          'inventor_country']

dfs = patentsvsiew_query_to_dfs(fields=fields, 
                                startdate='2019-01-01', 
                                enddate='2019-02-01', 
                                per_page=10000, 
                                force_retry=True, 
                                time_retry=1)
                              
for group, df in dfs.items():
    df.to_csv('YOUR/PATH/HERE/{}.csv'.format(group), index=False)
```
