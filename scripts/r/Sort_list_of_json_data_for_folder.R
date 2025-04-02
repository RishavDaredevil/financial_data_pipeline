# Install required packages if not already installed
if (!require(jsonlite)) install.packages("jsonlite")
if (!require(lubridate)) install.packages("lubridate")

library(jsonlite)
library(lubridate)

# Define the directory containing your JSON files
directory <- "D:/Desktop/financial_data_pipeline/data/raw/investing_com/investing_com_india_bonds_data"

# List all JSON files in the directory
json_files <- list.files(path = directory, pattern = "\\.json$", full.names = TRUE)

# Process each file
for (file in json_files) {
    # Read the JSON file
    data <- fromJSON(file)
    
    # Check if the data is a data frame or list that can be converted into a data frame
    if (is.data.frame(data) || is.list(data)) {
        # Convert to data frame if needed
        df <- as.data.frame(data)
        
        # Check if the "date" column exists
        if ("date" %in% names(df)) {
            # Convert date strings to Date objects for proper sorting
            df$date <- ymd(df$date)
            
            # Sort the data frame by the date column in ascending order
            df <- df[order(df$date), ]
            
            # Write the sorted data back to the file with pretty printing
            # toJSON by default returns a compact JSON so we use pretty = TRUE.
            sorted_json <- toJSON(df, pretty = TRUE, auto_unbox = TRUE)
            write(sorted_json, file)
            
            cat("Sorted and updated", file, "\n")
        } else {
            cat("The file", file, "does not contain a 'date' field.\n")
        }
    } else {
        cat("The file", file, "does not contain a valid JSON array of objects.\n")
    }
}
