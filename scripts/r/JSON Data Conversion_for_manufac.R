# Install required packages if needed:
# install.packages("readxl")
# install.packages("jsonlite")

library(readxl)
library(jsonlite)
library(tidyverse)

# Define the workbook path
wb_path <- "D:\\Downloads\\IOSR108FEB072025.xlsx"

# Function to convert survey quarter string to standard Y-m-d format.
# Mapping:
# Q1:YY-ZZ -> 20YY-05-01
# Q2:YY-ZZ -> 20YY-08-01
# Q3:YY-ZZ -> 20YY-11-01
# Q4:YY-ZZ -> 20YY-02-01
convert_quarter_date <- function(quarter_str) {
    # Use regex to extract the quarter number and year part
    m <- regexec("^Q([1-4]):(\\d\\d)-\\d\\d", quarter_str)
    matches <- regmatches(quarter_str, m)
    if (length(matches[[1]]) == 3) {
        q_num <- matches[[1]][2]
        year_part <- matches[[1]][3]
        if (q_num == 4) {year_part <- as.numeric(year_part) + 1} 
        # Define month-day mapping based on quarter number
        md <- switch(q_num,
                     "1" = "06-01",
                     "2" = "09-01",
                     "3" = "12-01",
                     "4" = "03-01",
                     "01-01")  # default fallback
        return(paste0("20", year_part, "-", md))
    }
    # Return the original string if pattern doesn't match
    return(quarter_str)
}

convert_quarter_date2 <- function(quarter_str) {
    # Regex to match a quarter (Q1-Q4), followed by a colon,
    # a four-digit year, a dash, then two digits:
    # e.g. "Q4:2020-21"
    # Capture groups:
    #   [1] -> quarter number (1, 2, 3, or 4)
    #   [2] -> first 4-digit year (e.g. 2020)
    #   [3] -> second 2-digit year (e.g. 21) -- unused below
    pattern <- "^Q([1-4]):(\\d{4})-(\\d{2})$"
    
    m <- regexec(pattern, quarter_str)
    matches <- regmatches(quarter_str, m)
    
    # If our pattern found a match, we get 4 pieces: 
    # 0: entire string, 1: Q#, 2: first year, 3: second year
    if (length(matches[[1]]) == 4) {
        q_num <- matches[[1]][2]         # "1", "2", "3", or "4"
        first_year <- matches[[1]][3]    # e.g. "2020"
        # second_year <- matches[[1]][4] # e.g. "21" (ignored here)
        
        # Map quarter number to month-day
        md <- switch(q_num,
                     "1" = "06-01",
                     "2" = "09-01",
                     "3" = "12-01",
                     "4" = "03-01",
                     "01-01")  # fallback
        
        # Construct final date: "YYYY-MM-DD"
        return(paste0(first_year, "-", md))
    }
    
    # If it doesn't match the pattern, return original string
    return(quarter_str)
}

# Get all sheet names
all_sheets <- excel_sheets(wb_path)

# Filter sheets whose names start with "Table S"
target_sheets_manufacturing <- all_sheets[grepl("^Table", all_sheets)]
# Create an empty list to hold the result JSON structure.
result <- list()

for (sheet in target_sheets_manufacturing) {
    if (sheet == "Table 23") {
        table_name <- read_excel(wb_path, sheet = sheet, range = "A1", col_names = FALSE)[[1]]
        Category <- "Manufacturing"
        table_name <- table_name |> str_remove(pattern = ".*: ")
        df <- read_excel(wb_path, sheet = sheet, skip = 1)
        # Rename columns for S14
        colnames(df) <- c("Survey_Quarter","Business_Assessment_Index", "Business_Expectations_Index")
    
        df <- df |> filter(str_detect(string = Survey_Quarter,pattern = "^Q"))
        # Iterate rows
        for (i in seq_len(nrow(df))) {
            # Convert "Survey conducted during" to a date
            quarter_key <- convert_quarter_date2(df$Survey_Quarter[i])
            
            if (is.null(result[[quarter_key]])) {
                result[[quarter_key]] <- list()
            }
            
            # Store data under table_name
            result[[quarter_key]][[Category]][[table_name]] <- list(
                "Business Assessment Index (BAI)" = df$Business_Assessment_Index[i],
                "Business Expectations Index (BEI)" = df$Business_Expectations_Index[i+1]
            )
        }
    }
    else {
        # Read the table name from cell B1
        table_name <- read_excel(wb_path, sheet = sheet, range = "B1", col_names = FALSE)[[1]] 
        Category <- "Manufacturing"
        
        table_name <- table_name |> str_remove(pattern = ".*: |.*-|.*:")
        
        # Read the data table.
        # Here we assume that the header row (with the column names) is on row 4.
        df <- read_excel(wb_path, sheet = sheet, skip = 1)
        df <- df[-1,]
        while (dim(df)[2]>9) {
            df <- df[-10]   
        }
        # Rename columns if they are not already correct.
        colnames(df) <- c("Survey_Quarter", "Assessment_Increase", 
                          "Assessment_Decrease", "Assessment_NoChange", "Assessment_NR",
                          "Expectation_Increase", 
                          "Expectation_Decrease", "Expectation_NoChange", "Expectation_NR")
        df <- df |> select(Survey_Quarter,Assessment_NR,Expectation_NR)
        
        df <- df |> filter(str_detect(string = Survey_Quarter,pattern = "^Q"))
        
        # Loop over each row of the data frame
        for (i in 1:nrow(df)) {
            quarter <- convert_quarter_date2(df$Survey_Quarter[i])
            
            # If this survey quarter doesn't exist yet, create a new list for it.
            if (is.null(result[[quarter]])) {
                result[[quarter]] <- list()
            }
            
            # Add the table data under the table name.
            result[[quarter]][[Category]][[table_name]] <- list(
                "Assessment - Net Res" = df$Assessment_NR[i],
                "Expectation for 1 qtr ahead - Net Res" = df$Expectation_NR[i+1]
            )
        }
    }
}

# Write JSONL file
jsonl_path <- "D:\\Desktop\\financial_data_pipeline\\data\\raw\\RBI_data\\manufacturing_survey.jsonl"
file_conn <- file(jsonl_path, "w")

for (quarter_key in names(result)) {
    entry <- list(date = quarter_key, data = result[[quarter_key]])
    writeLines(toJSON(entry, auto_unbox = TRUE), file_conn)
}

close(file_conn)
cat(paste("JSONL file saved to:", jsonl_path))

