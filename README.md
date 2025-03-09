# Financial Data Pipeline

This project is an automated data scraping and processing tool that collects economic and financial data from various sources. It gathers data on company financials, monetary policy, bond markets, stock prices, and more. All collected data is organized into a structured directory for easy access, cleaning, analysis, and dashboard creation.

## Project Overview

- **Web Scraping:** Uses Scrapy to scrape data from multiple sources and reverse engineers over 30 APIs from different domains.
- **Data Management:** Stores data in JSONL format, making it easy to process and analyze.
- **Automation & Scheduling:** Deployed on a home server using Scrapyd to schedule spiders at specific intervals.
- **Monitoring & Alerts:** Integrates ScrapydWeb for monitoring and uses ngrok for remote access. Log parsers are used to alert for any scraping issues.
- **Data Processing:** Includes both Python and R scripts for cleaning, analysis, and dashboard generation.
- **Version Control:** All work is maintained under Git, with an automated setup script to streamline environment configuration.

## Folder Structure

```plaintext
/financial_data_pipeline/
├── /scrapers/           # Contains all scraping scripts
│   ├── /python/         # Python scrapers
│   ├── /r/              # R scrapers
│   └── fetch_market_data.py
├── /data/               # Collected data
│   ├── /raw/            # Unprocessed data
│   ├── /processed/      # Cleaned data
│   └── /final/          # Data ready for analysis/dashboarding
├── /scripts/            # Data cleaning and analysis scripts
│   ├── /python/
│   │   ├── clean_data.py
│   │   ├── analyze_data.py
│   │   └── generate_dashboard.py
│   └── /r/
│       ├── clean_data.R
│       └── analyze_data.R
├── /dashboards/         # Dashboard files (PowerBI, Tableau, Streamlit)
├── /logs/               # Logs for debugging scrapers and scripts
├── /envs/               # Virtual environments for Python & R
├── /backups/            # Backup storage
├── .gitignore           # Files and folders to ignore in Git
├── requirements.txt     # Python dependencies
├── renv.lock            # R package dependencies
├── README.md            # Project documentation (this file)
└── setup.sh             # Automated setup script for environments
```

## Setup and Installation

### Clone the Repository

```bash
git clone https://github.com/your-username/financial_data_pipeline.git
cd financial_data_pipeline
```

### Set Up Virtual Environments

**Python:**

```bash
cd envs/python_env
# On Linux/Mac
source bin/activate
# On Windows
.\Scripts\activate
pip install -r ../../requirements.txt
```

**R:**

Open the project in RStudio and install the necessary packages using the `renv.lock` file.

### Run the Setup Script

```bash
bash setup.sh
```

## Usage

- **Scraping Data:** Navigate to the scrapers folder and run a desired spider. For example:

  ```bash
  scrapy runspider scrapers/python/fetch_market_data.py
  ```

- **Data Processing & Analysis:** Use the Python or R scripts in the `scripts` folder to clean and analyze the scraped data.
- **Dashboard:** Create interactive dashboards using the files in the `dashboards` folder (compatible with PowerBI, Tableau, and Streamlit).

### Example Spider

Below is a snippet from one of the spider scripts:

```python
import scrapy


class RbiServicesAndInfrastructureSurveySpider(scrapy.Spider):
    name = "rbi_services_and_infrastructure_survey"
    allowed_domains = ["website.rbi.org.in"]
    start_urls = [
        "https://website.rbi.org.in/web/rbi/publications/articles?category=24927118&delta=100"
    ]

    # Additional scraping logic here...
```

For a complete example, refer to the Python scrapers in `/scrapers/python/`.

## Contributing

Contributions are welcome! Feel free to fork the repository, make improvements, and submit a pull request. For any issues or suggestions, please open an issue on GitHub.

## License

This project is licensed under the MIT License.

## Contact

If you have any questions or need further information, please contact [your-email@example.com] or open an issue on this repository.