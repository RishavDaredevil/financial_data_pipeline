library(jsonlite)

# Set the directory containing your JSON files
directory <- "D:/Desktop/financial_data_pipeline/data/raw/fxempire_data/commodities"

# List all JSON files in the directory (full paths)
json_files <- list.files(directory, pattern = "\\.json$", full.names = TRUE)

# Initialize an empty list to store the latest date data for each file
latest_data_list <- list()

# Process each JSON file
for (file in json_files) {
    # Read the JSON data from the file
    data <- fromJSON(file)
    
    # Ensure the data is a data frame (an array of objects)
    if (!is.data.frame(data)) {
        warning(paste("Skipping", file, "- data is not in the expected format."))
        next
    }
    
    # Convert the 'date' column to Date type for proper comparison
    data$date <- as.Date(data$date, format = "%Y-%m-%d")
    
    # Determine the latest date in this file
    max_date <- max(data$date, na.rm = TRUE)
    
    # Extract the row(s) that match the latest date
    latest_rows <- data[data$date == max_date, ]
    
    # Optionally, add a column with the source file name for reference
    latest_rows$source_file <- basename(file)
    
    # Append the result to the list
    latest_data_list[[length(latest_data_list) + 1]] <- latest_rows
}

# Combine all latest rows from each file into one data frame
latest_data <- do.call(rbind, latest_data_list)

# Display the aggregated latest date data
print(latest_data)
