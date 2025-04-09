setwd("D:/Desktop/financial_data_pipeline/scripts/r")

# Install and load required packages if not already installed
if (!requireNamespace("readxl", quietly = TRUE)) install.packages("readxl")
if (!requireNamespace("jsonlite", quietly = TRUE)) install.packages("jsonlite")
if (!requireNamespace("stringr", quietly = TRUE)) install.packages("stringr")

library(readxl)
library(jsonlite)
library(stringr)

# Specify the directory containing the Excel files
directory <- "D:\\Desktop\\Trendlyne\\Cleaned"

# Get list of all Excel files in the directory (full paths)
files <- list.files(path = directory, pattern = "\\.xlsx$", full.names = TRUE)

# Output file for JSONL lines
output_file <- "all_data.jsonl"

# Define a regex pattern to capture a date in dd-mm-yyyy format in the filename
# The pattern captures day, month and year as separate groups
date_pattern <- "(\\d{2})-(\\d{2})-(\\d{4})"

# Define the expected keys for each record
expected_keys <- c(
    "Stock",
    "Market Capitalization",
    "Annual 2y forward forecaster estimates EPS",
    "Annual 1y forward forecaster estimates EPS",
    "Basic EPS Annual",
    "Basic EPS Annual 1Yr Ago",
    "Forecaster Estimates industry reco",
    "Sector",
    "Industry",
    "Current Price",
    "Annual 2y forward forecaster estimates Revenue",
    "Annual 1y forward forecaster estimates Revenue",
    "Total Revenue Annual",
    "EnterpriseValue Annual",
    "EBIT Annual Per Share",
    "EV Per EBITDA Annual",
    "Outstanding Shares Adjusted",
    "Stock code",
    "ISIN",
    "BSE code",
    "NSE code"
)

# Initialize a list to hold all records
records_list <- list()

# Loop over all Excel files found
for(file in files) {
    
    # Extract the base filename for regex processing
    filename <- basename(file)
    
    # Exclude files with "wrong" in their filename
    if(str_detect(tolower(filename), "wrong")) {
        message(paste("Skipping file (contains 'wrong'): ", filename))
        next
    }
    
    # Search for the date pattern in the filename
    date_match <- str_match(filename, date_pattern)
    
    if(is.na(date_match[1,1])) {
        message(paste("No date found in filename:", filename, "- skipping file"))
        next
    }
    
    # Extract the day, month and year from the match
    day <- date_match[1,2]
    month <- date_match[1,3]
    year <- date_match[1,4]
    
    # Convert to an ISO-like format: yyyy-mm-dd
    date_str <- paste(year, month, day, sep = "-")
    
    # Attempt to read the Excel file
    df <- tryCatch({
        read_excel(file)
    }, error = function(e) {
        message(paste("Error reading", filename, ":", e$message))
        return(NULL)
    })
    
    # If reading the Excel file failed, skip to the next file
    if(is.null(df)) next
    
    # Process each row to create a list with the fixed schema:
    data_list <- lapply(seq_len(nrow(df)), function(i) {
        row_data <- df[i, ]
        record_row <- list()
        # For each expected key, check if it exists in the row; if not, assign NA (which becomes null in JSON)
        for(key in expected_keys) {
            if(key %in% colnames(row_data)) {
                record_row[[key]] <- row_data[[key]]
            } else {
                record_row[[key]] <- NA
            }
        }
        record_row
    })
    
    # Convert the data frame to a list of records and replace NA values with NULL in JSON
    # The toJSON() function with na = "null" ensures that NA becomes null in JSON
    # Also, auto_unbox = TRUE prevents vectors of length one from being wrapped as arrays.
    record <- list(
        date = date_str,
        data = df
    )
    
    # Append the record to our list of records
    records_list[[length(records_list) + 1]] <- record
}
 
# Sort the records by the "date" key in ascending order.
# Since dates are in "yyyy-mm-dd" format, they can be sorted as strings.
records_list <- records_list[order(sapply(records_list, function(x) x$date))]

# Open the output file connection
output_conn <- file(output_file, open = "wt", encoding = "UTF-8")

# Write each sorted record as a separate line (JSONL)
for(record in records_list) {
    record_json <- toJSON(record, na = "null", auto_unbox = TRUE)
    writeLines(record_json, output_conn)
}

# Close the output file connection
close(output_conn)

message(paste("Processing complete. Data saved to", output_file))




