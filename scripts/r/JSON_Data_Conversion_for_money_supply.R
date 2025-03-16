# Load necessary libraries
library(readxl)
library(jsonlite)
library(tidyverse)

# Define file paths and sheet name
file_path <- "D:/Downloads/RBIB Table No. 06 _ Money Stock Measures.xlsx"
sheet_name <- "Mydata"
json_output_path <- "D:/Downloads/money_supply.jsonl"

# Wrap operations in a tryCatch for error handling
tryCatch({
    
    # Read the specified sheet from the Excel file
    df <- read_excel(file_path, sheet = sheet_name,col_types = c("date",rep("numeric",10)))
    
    # Strip numbers from the left side of column names using a regular expression
    colnames(df) <- gsub("^\\d+(\\.\\d+)?\\s*", "", colnames(df))
    
    # Convert the data frame into a list, keyed by the row names (dates)
    df_list <- split(df |> select(-Date), df$Date)
    
    # Open a connection to the output JSONL file
    con <- file(json_output_path, open = "w", encoding = "UTF-8")
    
    # Iterate through each element of the list and write it as a separate JSON object per line
    for (name in names(df_list)) {
        # Convert the row (as a list) into a JSON object with the date as the key
        json_obj <- toJSON(setNames(list(as.list(df_list[[name]])), name), auto_unbox = TRUE)
        writeLines(json_obj, con)
    }
    
    # Close the connection
    close(con)
    
    cat("Conversion successful. JSONL file saved to:", json_output_path, "\n")

}, error = function(e) {
    cat("Error:", e$message, "\n")
})
