import os
import json
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from dotenv import load_dotenv
from seeking_alpha_utils import *

load_dotenv()  # Loads variables from .env

def get_news(n=20):
    url = "https://seeking-alpha.p.rapidapi.com/news/v2/list"

    querystring = {"size": f"{n}", "category": "market-news::all", "number": "1"}

    headers = {
        "x-rapidapi-key": os.getenv('SEEKING_ALPHA_API_KEY'),
        "x-rapidapi-host": "seeking-alpha.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)

    return response.json()



def extract_news(news_json):
    """
    Extracts useful information from the provided news JSON.

    Parameters:
        news_json (dict): The JSON data containing news articles and included items.

    Returns:
        list: A list of dictionaries, each containing extracted information for a news article.
    """
    data = news_json.get('data', [])
    included = news_json.get('included', [])

    # Create mappings for included items (authors and tags)
    included_map = {item['id']: item for item in included}

    articles = []

    for item in data:
        article = {}
        attributes = item.get('attributes', {})
        relationships = item.get('relationships', {})
        links = item.get('links', {})

        # Basic article information
        article['id'] = item.get('id')
        article['title'] = attributes.get('title')
        article['published_on'] = parse_datetime(attributes.get('publishOn'))
        article['last_modified'] = parse_datetime(attributes.get('lastModified'))
        # article['Status'] = attributes.get('status')
        # article['Is Paywalled'] = attributes.get('isPaywalled')
        # article['Comment Count'] = attributes.get('commentCount')
        article['content'] = get_text_from_html(attributes.get('content'))
        # article['Getty Image URL'] = attributes.get('gettyImageUrl')
        # article['URI Image'] = links.get('uriImage')
        article['url'] = links.get('canonical')
        # article['url'] = links.get('self')

        # Primary Tickers
        primary_tickers = relationships.get('primaryTickers', {}).get('data', [])
        article['tickers_primary'] = []
        for ticker in primary_tickers:
            ticker_id = ticker.get('id')
            ticker_info = included_map.get(ticker_id, {}).get('attributes', {})
            article['tickers_primary'].append(ticker_info.get('name'))

        # Secondary Tickers
        secondary_tickers = relationships.get('secondaryTickers', {}).get('data', [])
        article['tickers_secondary'] = []
        for ticker in secondary_tickers:
            ticker_id = ticker.get('id')
            ticker_info = included_map.get(ticker_id, {}).get('attributes', {})
            article['tickers_secondary'].append(ticker_info.get('name'))

        articles.append(article)

    return articles


# Fetch all articles
def fetch_all_news(n=20):
    # Get news
    news_json = get_news(n)
    all_articles = extract_news(news_json)

    return all_articles

