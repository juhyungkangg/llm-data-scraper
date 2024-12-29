from seeking_alpha_article_fetcher import *
from seeking_alpha_news_fecther import *
import time
import schedule
from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime, timedelta
import random

def main():
    news = fetch_all_news(30)
    articles = fetch_all_articles(30)

    connection = create_connection()
    for news_item in news:
        insert_seeking_alpha_data(connection, news_item, 4)
    for article in articles:
        insert_seeking_alpha_data(connection, article, 5)

    connection.close()
    print("MySQL connection closed.")

    cnt = len(news) + len(articles)

    formatted_time = datetime.now().strftime('%Y-%m-%d %I:%M %p')
    print(f"Uploaded {cnt} articles in seeking alpha db at {formatted_time}. ")

# Schedule the function to run at specific times
for hour in range(24):
    time_str = f"{hour:02d}:40"
    schedule.every().day.at(time_str).do(main)

if __name__ == '__main__':
    while True:
        schedule.run_pending()
        time.sleep(1)


