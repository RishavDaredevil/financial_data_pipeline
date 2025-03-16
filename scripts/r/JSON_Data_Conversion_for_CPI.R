
# Load necessary libraries
library(readxl)
library(jsonlite)
library(tidyverse)

# Define file paths and sheet name
file_path <- "D:/Downloads/RBIB Table No. 18 _ Consumer Price Index (Base 2010=100).xlsx"
json_output_path <- "D:/Downloads/CPI-Rural_Urban_Combined.json"

#' Recursively split a data frame by each of its first columns
#'
#' @param df A data frame to be split
#' @param how_many_columns_to_nest The number of times to recursively split by the first column
#'
#' @return A nested list, where each level corresponds to the split by one column
#'
nest_split <- function(df, how_many_columns_to_nest) {
    # If no more nesting is requested or there are no columns left, return df as-is
    if (how_many_columns_to_nest == 0 || ncol(df) == 0) {
        return(df)
    }
    
    # Split the data frame by the values in the first column
    splitted <- split(df[, -1, drop = FALSE], df[, 1])
    
    # Recursively call nest_split on each subset with one fewer split level
    splitted <- lapply(splitted, function(subdf) {
        nest_split(subdf, how_many_columns_to_nest - 1)
    })
    
    splitted
}

# Example usage:
# Suppose 'df' has columns: "Col1", "Col2", "Col3", "Col4"
# If you call nest_split(df, 2), it will:
# 1) Split df by df$Col1, remove Col1
# 2) For each subset, split again by the new first column (originally Col2), remove it
# 3) Return a nested list structure


# Wrap operations in a tryCatch for error handling
tryCatch({
    
    # Get all sheet names
    all_sheets <- excel_sheets(file_path)
    
    
    # Filter sheets whose names start with "Table S"
    target_sheets_manufacturing <- all_sheets[grepl("^CPI - 2012", all_sheets)]
    df_full_list <- list()
    for (sheet in target_sheets_manufacturing) {
        
        
        # Read the specified sheet from the Excel file
        df <- read_excel(file_path, sheet = sheet,skip = 5)
        
        # Strip numbers from the left side of column names using a regular expression
        colnames(df) <- c(
            "Month",
            "Commodity Description",
            "Provisional / Final",
            "Rural-Index",
            "Rural-Inflation",
            "Urban-Index",
            "Urban-Inflation",
            "Combined-Index",
            "Combined-Inflation"
        )
        if (is.na(df[1,1])) {
            df <- df[-1,] |> filter(str_detect(string = Month,pattern = "^Note",negate = T)) 
        }
        df <- df |> mutate(Date = my(Month)) |> select(-Month) |> relocate(Date)
        
        # Convert the data frame into a list, keyed by the row names (dates)
        df_list <- nest_split(df,how_many_columns_to_nest = 3)
        
        df_full_list <- list(purrr::list_flatten(df_full_list),df_list)
        
        print("ok")
    }
    # Open a connection to the output JSONL file
    con <- file(json_output_path, open = "w", encoding = "UTF-8")
    
    writeLines(toJSON(x = purrr::list_flatten(df_full_list),auto_unbox = T,pretty = T), con)
    
    # Close the connection
    close(con)
    
    cat("Conversion successful. JSONL file saved to:", json_output_path, "\n")
    

}, error = function(e) {
    cat("Error:", e$message, "\n")
})
