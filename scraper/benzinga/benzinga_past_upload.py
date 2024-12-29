from scraper.benzinga.benzinga_scraper import *
import pandas as pd

if __name__ == '__main__':
    date = "2010-01-01"
    end_date = datetime.now() - timedelta(days=10)

    while pd.to_datetime(date) <= end_date:
        max_date = main(date)
        print(f'Done with {date}')

        next_date = max_date - timedelta(days = 2)
        date = next_date.strftime('%Y-%m-%d')
