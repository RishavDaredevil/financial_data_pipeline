# Macroeconomic Data Scraper - Trading Economics (India)

This repository contains scraped data for various macroeconomic indicators for India, sourced from Trading Economics.

## Data Tracking

A CSV file named `current_macro_data_trading_eco_last_date.csv` is maintained in this project. This file tracks the 'Last' date associated with each scraped indicator at the time of the scrape, helping to monitor data updates.

## Indicator Availability and Scrape Status

Based on the analysis of the Trading Economics India page:

### Total Unique Indicators Available on Site: 119

(List generated from the HTML structure provided)

1.  Average Weekly Hours
2.  Balance of Trade
3.  Bank Lending Rate
4.  Business Confidence
5.  Capacity Utilization
6.  Capital Flows
7.  Car Production
8.  Car Registrations
9.  Cash Reserve Ratio
10. Central Bank Balance Sheet
11. Changes in Inventories
12. Composite Leading Indicator
13. Composite PMI
14. Construction Output
15. Consumer Confidence
16. Consumer Price Index CPI
17. Consumer Spending
18. Corporate Tax Rate
19. Corruption Index
20. Corruption Rank
21. CPI Housing Utilities
22. CPI Transportation
23. Credit Rating  <-- *Not Scraped*
24. Crude Oil Production
25. Currency <-- *Not Scraped*
26. Current Account
27. Current Account to GDP
28. Deposit Growth
29. Disposable Personal Income
30. Electricity Production
31. Employment Rate
32. Export Prices
33. Exports
34. External Debt
35. Fiscal Expenditure
36. Food Inflation
37. Foreign Direct Investment
38. Foreign Exchange Reserves
39. Full Year GDP Growth
40. Gasoline Prices
41. GDP
42. GDP Annual Growth Rate
43. GDP Constant Prices
44. GDP Deflator
45. GDP from Agriculture
46. GDP from Construction
47. GDP from Manufacturing
48. GDP from Mining
49. GDP from Public Administration
50. GDP from Utilities
51. GDP Growth Rate
52. GDP per Capita
53. GDP per Capita PPP
54. Gold Reserves
55. Government Budget
56. Government Budget Value
57. Government Debt to GDP
58. Government Revenues
59. Government Spending
60. Government Spending to GDP
61. Gross Fixed Capital Formation
62. Gross National Product
63. Hospital Beds
64. Households Debt to GDP
65. Housing Index
66. Import Prices
67. Imports
68. Industrial Production
69. Industrial Production Mom
70. Inflation Expectations
71. Inflation Rate
72. Inflation Rate MoM
73. Interbank Rate
74. Interest Rate
75. Labor Force Participation Rate
76. Loan Growth
77. Manufacturing PMI
78. Manufacturing Production
79. Military Expenditure
80. Minimum Wages
81. Mining Production
82. Money Supply M0
83. Money Supply M1
84. Money Supply M2
85. Money Supply M3
86. Personal Income Tax Rate
87. Population
88. Producer Price Inflation MoM
89. Producer Prices
90. Producer Prices Change
91. Remittances
92. Residential Property Prices
93. Retirement Age Men
94. Retirement Age Women
95. Reverse Repo Rate
96. Sales Tax Rate
97. Services PMI
98. Social Security Rate
99. Social Security Rate For Companies
100. Social Security Rate For Employees
101. Steel Production
102. Stock Market <-- *Not Scraped*
103. Terms of Trade
104. Terrorism Index
105. Total Vehicle Sales
106. Tourist Arrivals
107. Unemployment Rate
108. Wages
109. Weapons Sales
110. Withholding Tax Rate
111. WPI Food Index YoY
112. WPI Fuel YoY
113. WPI Manufacturing YoY
... (and the 3 missing ones listed above make up the 119)

### Indicators Successfully Scraped: 114

(Based on the provided list of .json filenames)

1.  Auto Exports
2.  Average Weekly Hours
3.  Balance of Trade
4.  Bank Lending Rate
5.  Business Confidence
6.  Capacity Utilization
7.  Capital Flows
8.  Car Production
9.  Car Registrations
10. Cash Reserve Ratio
11. Central Bank Balance Sheet
12. Changes in Inventories
13. Composite Leading Indicator
14. Composite PMI
15. Construction Output
16. Consumer Confidence
17. Consumer Price Index CPI
18. Consumer Spending
19. Corporate Tax Rate
20. Corruption Index
21. Corruption Rank
22. CPI Housing Utilities
23. CPI Transportation
24. Crude Oil Production
25. Current Account
26. Current Account to GDP
27. Deposit Growth
28. Disposable Personal Income
29. Electricity Production
30. Employment Rate
31. Export Prices
32. Exports
33. External Debt
34. Fiscal Expenditure
35. Food Inflation
36. Foreign Direct Investment
37. Foreign Exchange Reserves
38. Full Year GDP Growth
39. Gasoline Prices
40. GDP
41. GDP Annual Growth Rate
42. GDP Constant Prices
43. GDP Deflator
44. GDP from Agriculture
45. GDP from Construction
46. GDP from Manufacturing
47. GDP from Mining
48. GDP from Public Administration
49. GDP from Utilities
50. GDP Growth Rate
51. GDP per Capita
52. GDP per Capita PPP
53. Gold Reserves
54. Government Budget
55. Government Budget Value
56. Government Debt to GDP
57. Government Revenues
58. Government Spending
59. Government Spending to GDP
60. Gross Fixed Capital Formation
61. Gross National Product
62. Hospital Beds
63. Households Debt to GDP
64. Housing Index
65. Import Prices
66. Imports
67. Industrial Production
68. Industrial Production Mom
69. Inflation Expectations
70. Inflation Rate
71. Inflation Rate MoM
72. Interbank Rate
73. Interest Rate
74. Labor Force Participation Rate
75. Loan Growth
76. Manufacturing PMI
77. Manufacturing Production
78. Military Expenditure
79. Minimum Wages
80. Mining Production
81. Money Supply M0
82. Money Supply M1
83. Money Supply M2
84. Money Supply M3
85. Personal Income Tax Rate
86. Population
87. Producer Price Inflation MoM
88. Producer Prices
89. Producer Prices Change
90. Remittances
91. Residential Property Prices
92. Retirement Age Men
93. Retirement Age Women
94. Reverse Repo Rate
95. Sales Tax Rate
96. Services PMI
97. Social Security Rate
98. Social Security Rate For Companies
99. Social Security Rate For Employees
100. Steel Production
101. Terms of Trade
102. Terrorism Index
103. Total Vehicle Sales
104. Tourist Arrivals
105. Unemployment Rate
106. Wages
107. Weapons Sales
108. Withholding Tax Rate
109. WPI Food Index YoY
110. WPI Fuel YoY
111. WPI Manufacturing YoY

### Indicators Not Scraped: 3

The following indicators available on the page were **not** found in the list of scraped files:

1.  Currency
2.  Stock Market
3.  Credit Rating