import json
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from dotenv import load_dotenv
import os
from seeking_alpha_utils import *


load_dotenv()  # Loads variables from .env




# Get article list
def get_article_list(n=20):
    url = "https://seeking-alpha.p.rapidapi.com/articles/v2/list"

    querystring = {"size": f"{n}", "number": "1", "category": "latest-articles"}

    headers = {
        "x-rapidapi-key": os.getenv('SEEKING_ALPHA_API_KEY'),
        "x-rapidapi-host": "seeking-alpha.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)

    return response.json()


# Get article body
def get_article_details(article_id):
    import requests

    url = "https://seeking-alpha.p.rapidapi.com/articles/get-details"

    querystring = {"id": f"{article_id}"}

    headers = {
        "x-rapidapi-key": os.getenv('SEEKING_ALPHA_API_KEY'),
        "x-rapidapi-host": "seeking-alpha.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)

    return response.json()


# Extract ID
def extract_id(data):
    """
    Extracts article information including id, title, primary tickers, and secondary tickers.

    Parameters:
        data (dict): The data dictionary containing articles and included items.

    Returns:
        list of dict: A list where each dictionary contains the 'id', 'title', 'primary_tickers', and 'secondary_tickers' of an article.
    """
    articles = []

    # Iterate over each article in the data
    for article in data.get('data', []):
        attributes = article.get('attributes', {})

        article_id = article.get('id')

        # Append the extracted information to the articles list
        articles.append({
            'id': article_id,
        })

    return articles


# Extract article detail
def extract_article_detail(api_response):
    # Initialize containers for extracted data
    tickers_primary = []
    tickers_secondary = []
    other_tags = []

    # Create a mapping from tag ID to tag details for quick lookup
    tag_id_mapping = {}
    included = api_response.get('included', [])
    for item in included:
        if item.get('type') == 'tag':
            tag_id = item.get('id')
            attributes = item.get('attributes', {})
            tag_details = {
                'slug': attributes.get('slug'),
                'name': attributes.get('name'),
                'company': attributes.get('company'),
                'exchange': attributes.get('exchange'),
                'currency': attributes.get('currency'),
                'equityType': attributes.get('equityType')
            }
            tag_id_mapping[tag_id] = tag_details
        elif item.get('type') == 'author':
            # Extract author information
            author_id = item.get('id')
            attributes = item.get('attributes', {})
            author_info = {
                'author_id': author_id,
                'nick': attributes.get('nick'),
                'bio': attributes.get('bio'),
                'followers_count': attributes.get('followersCount'),
                'profile_url': item.get('links', {}).get('profileUrl')
            }

    # Extract article data
    data = api_response.get('data', {})
    attributes = data.get('attributes', {})
    relationships = data.get('relationships', {})

    # Extract tickers
    # Primary Tickers
    primary_tickers = relationships.get('primaryTickers', {}).get('data', [])
    for ticker in primary_tickers:
        tag_id = ticker.get('id')
        if tag_id in tag_id_mapping:
            tickers_primary.append(tag_id_mapping[tag_id]['name'])

    # Secondary Tickers
    secondary_tickers = relationships.get('secondaryTickers', {}).get('data', [])
    for ticker in secondary_tickers:
        tag_id = ticker.get('id')
        if tag_id in tag_id_mapping:
            tickers_secondary.append(tag_id_mapping[tag_id]['name'])

    # Other Tags
    other_tags_data = relationships.get('otherTags', {}).get('data', [])
    for tag in other_tags_data:
        tag_id = tag.get('id')
        if tag_id in tag_id_mapping:
            other_tags.append(tag_id_mapping[tag_id])

    # Compile all extracted information
    extracted_data = {
        'id': data.get('id'),
        'title': attributes.get('title'),
        'published_on': parse_datetime(attributes.get('publishOn')),
        'last_modified': parse_datetime(attributes.get('lastModified')),
        # 'comments_count': attributes.get('commentCount'),
        # 'likes_count': attributes.get('likesCount'),
        # 'is_paywalled': attributes.get('isPaywalled'),
        # 'status': attributes.get('status'),
        # 'author': author_info,
        # 'themes': attributes.get('themes'),
        'summary': ' '.join(attributes.get('summary')) if attributes.get('summary') else None,
        # 'disclosure': attributes.get('disclosure'),
        'content': get_text_from_html(attributes.get('content')),
        # 'twitContent': attributes.get('twitContent')
        'tickers_primary': tickers_primary,
        'tickers_secondary': tickers_secondary,
        # 'other_tags': other_tags,
        'url': data.get('links', {}).get('canonical',None)
    }

    return extracted_data


# Fetch all articles
def fetch_all_articles(n=20):
    # Get list
    articles_json = get_article_list(n)
    articles = extract_id(articles_json)

    all_articles = []
    # Get body
    for article in articles:
        article_json = get_article_details(article['id'])
        article_data = extract_article_detail(article_json)
        if article_data['id'] is None:
            continue
        all_articles.append(article_data)

    return all_articles
