# ETL with Python: From Raw Amazon Sales to Clean Insights

This project showcases a practical ETL (Extract, Transform, Load) workflow using **Python**, applied to a raw Amazon sales dataset.  
It demonstrates essential **data wrangling skills** used to clean messy, incomplete, and duplicate records and turn them into a clean, analysis-ready dataset.

## ETL process

### Check missing values
missing_values = df.isnull().sum()

```csv
index                     0
Order ID                  0
Date                      0
Status                    0
Fulfilment                0
Sales Channel             0
ship-service-level        0
Style                     0
SKU                       0
Category                  0
Size                      0
ASIN                      0
Courier Status         6872
Qty                       0
currency               7795
Amount                 7795
ship-city                33
ship-state               33
ship-postal-code         33
ship-country             33
promotion-ids         49153
B2B                       0
fulfilled-by          89698
Unnamed: 22           49050
dtype: int64
```


### Check unique values and counts
unique_values_count = df.nunique().to_frame(name='Count of unique values')
unique_values = df.apply(pd.unique).to_frame(name='Unique Values')

```csv
Count of unique values
index                               128975
Order ID                            120378
Date                                    91
Status                                  13
Fulfilment                               2
Sales Channel                            2
ship-service-level                       2
Style                                 1377
SKU                                   7195
Category                                 9
Size                                    11
ASIN                                  7190
Courier Status                           3
Qty                                     10
currency                                 1
Amount                                1410
ship-city                             8955
ship-state                              69
ship-postal-code                      9459
ship-country                             1
promotion-ids                         5787
B2B                                      2
fulfilled-by                             1
Unnamed: 22                              1
```
```csv
Unique Values
index               [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13,...
Order ID            [405-8078784-5731545, 171-9198151-1101146, 404...
Date                [04-30-22, 04-29-22, 04-28-22, 04-27-22, 04-26...
Status              [Cancelled, Shipped - Delivered to Buyer, Ship...
Fulfilment                                         [Merchant, Amazon]
Sales Channel                                 [Amazon.in, Non-Amazon]
ship-service-level                              [Standard, Expedited]
Style               [SET389, JNE3781, JNE3371, J0341, JNE3671, SET...
SKU                 [SET389-KR-NP-S, JNE3781-KR-XXXL, JNE3371-KR-X...
Category            [Set, kurta, Western Dress, Top, Ethnic Dress,...
Size                 [S, 3XL, XL, L, XXL, XS, 6XL, M, 4XL, 5XL, Free]
ASIN                [B09KXVBD7Z, B09K3WFS32, B07WV4JV4D, B099NRCT7...
Courier Status                   [nan, Shipped, Cancelled, Unshipped]
Qty                                  [0, 1, 2, 15, 3, 9, 13, 5, 4, 8]
currency                                                   [INR, nan]
Amount              [647.62, 406.0, 329.0, 753.33, 574.0, 824.0, 6...
ship-city           [MUMBAI, BENGALURU, NAVI MUMBAI, PUDUCHERRY, C...
ship-state          [MAHARASHTRA, KARNATAKA, PUDUCHERRY, TAMIL NAD...
ship-postal-code    [400081.0, 560085.0, 410210.0, 605008.0, 60007...
ship-country                                                [IN, nan]
promotion-ids       [nan, Amazon PLCC Free-Financing Universal Mer...
B2B                                                     [False, True]
fulfilled-by                                         [Easy Ship, nan]
Unnamed: 22                                              [nan, False]
```

### Drop unnecessary columns
```csv
df.drop(columns=['index', 'Unnamed: 22', 'fulfilled-by', 'ship-country', 'currency'], inplace=True)

Index(['Order ID', 'Date', 'Status', 'Fulfilment', 'Sales Channel ',
       'ship-service-level', 'Style', 'SKU', 'Category', 'Size', 'ASIN',
       'Courier Status', 'Qty', 'Amount', 'ship-city', 'ship-state',
       'ship-postal-code', 'promotion-ids', 'B2B'],
      dtype='object')
```
### Strip any leading/trailing spaces from column names
df.columns = df.columns.str.strip()

### Remove duplicate rows based on 'Order ID' and 'ASIN'
```csv
df = df.drop_duplicates(subset=['Order ID', 'ASIN'])
128968
```

### Fill missing values for 'Amount'columns (fill with '0' for 'Amount')
```csv
df['Amount'].fillna(0,inplace=True)
```
### Fill missing with 'Unknown'
```csv
df['ship-city'].fillna('Unknown', inplace=True)
df['ship-state'].fillna('Unknown', inplace=True)
df['ship-postal-code'].fillna('Unknown', inplace=True)
df['Courier Status'].fillna('Unknown', inplace=True)
```
### Fill missing with 'No'
```csv
df['promotion-ids'].fillna('no',inplace=True)
```
### print result
missing_values_after = df.isnull().sum()
print(missing_values_after)

```csv
Order ID              0
Date                  0
Status                0
Fulfilment            0
Sales Channel         0
ship-service-level    0
Style                 0
SKU                   0
Category              0
Size                  0
ASIN                  0
Courier Status        0
Qty                   0
Amount                0
ship-city             0
ship-state            0
ship-postal-code      0
promotion-ids         0
B2B                   0
dtype: int64
```
