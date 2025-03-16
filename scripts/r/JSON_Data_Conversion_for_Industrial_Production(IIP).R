
# Load necessary libraries
library(readxl)
library(jsonlite)
library(tidyverse)

# Define file paths and sheet name
file_path <- "D:/Downloads/RBIB Table No. 22 _ Index of Industrial Production (Base_ 2004-05=100 & 2011-12).xlsx"
json_output_path <- "D:/Downloads/Index_of_Industrial_Production_(IIP-2011-12=100).json"

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
    target_sheets_manufacturing <- all_sheets
    df_full_list <- list()
    for (sheet in target_sheets_manufacturing) {
        
        
        # Read the specified sheet from the Excel file
        df <- read_excel(file_path, sheet = sheet,skip = 5)
        
        # Strip numbers from the left side of column names using a regular expression
        colnames(df) <- c(
            "Month",
            "General Index",
            "Sectoral Classification-Mining",
            "Sectoral Classification-Manufacturing",
            "Sectoral Classification-Electricity",
            "Use-Based Classification-Primary Goods",
            "Use-Based Classification-Capital Goods",
            "Use-Based Classification-Intermediate Goods",
            "Use-Based Classification-Infrastructure/Construction Goods",
            "Use-Based Classification-Consumer Durables",
            "Use-Based Classification-Consumer Non-Durables"
        )
        
        if (is.na(df[1,1])) {
            df <- df[-1,] |> filter(str_detect(string = Month,pattern = "^Note|^Weight",negate = T)) 
        }
        df <- df |> filter(str_detect(string = Month,pattern = "^\\d.*")) 
        df <- df |> mutate(Date = ym(Month)) |> select(-Month) |> relocate(Date)
        
        # Convert the data frame into a list, keyed by the row names (dates)
        df_list <- nest_split(df,how_many_columns_to_nest = 1)
        
        df_full_list <- list(purrr::list_flatten(df_full_list),df_list)
        
        print("ok")
    }

    jsontext = '{
        "weight(Sectoral Classification && Use-Based Classification are diff groups)": {
            "General Index": 100.00,
            "Sectoral Classification-Mining": 14.37,
            "Sectoral Classification-Manufacturing": 77.63,
            "Sectoral Classification-Electricity": 7.99,
            "Use-Based Classification-Primary Goods": 34.05,
            "Use-Based Classification-Capital Goods": 8.22,
            "Use-Based Classification-Intermediate Goods": 17.22,
            "Use-Based Classification-Infrastructure/Construction Goods": 12.34,
            "Use-Based Classification-Consumer Durables": 12.84,
            "Use-Based Classification-Consumer Non-Durables": 15.33
        }
    }'

    # tibble(Weight=c(100.00,14.37,77.63,7.99,34.05,8.22,17.22,12.34,12.84,15.33),names = c(
    #     "General Index",
    #     "Sectoral Classification-Mining",
    #     "Sectoral Classification-Manufacturing",
    #     "Sectoral Classification-Electricity",
    #     "Use-Based Classification-Primary Goods",
    #     "Use-Based Classification-Capital Goods",
    #     "Use-Based Classification-Intermediate Goods",
    #     "Use-Based Classification-Infrastructure/Construction Goods",
    #     "Use-Based Classification-Consumer Durables",
    #     "Use-Based Classification-Consumer Non-Durables"
    # ),)
    
    # 1. Parse the JSON
    parsed_json <- fromJSON(jsontext)
    
    # 2. Extract the single sub-list, unlist it to get a named vector,
    #    transpose it to make each key a column name
    df_key_value <- as.data.frame(t(unlist(parsed_json[[1]])))
    
    df_key_value <- list("weight(Sectoral Classification && Use-Based Classification are diff groups)" =
                             list(df_key_value))
    df_full_list <- list(list_flatten(df_key_value),purrr::list_flatten(df_full_list))

    # Open a connection to the output JSONL file
    con <- file(json_output_path, open = "w", encoding = "UTF-8")

    writeLines(toJSON(x = purrr::list_flatten(df_full_list),auto_unbox = T,pretty = T), con)

    # Close the connection
    close(con)

    cat("Conversion successful. JSONL file saved to:", json_output_path, "\n")


}, error = function(e) {
    cat("Error:", e$message, "\n")
})
