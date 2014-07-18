__author__ = 'faye'
import requests
import json
import sys
from bs4 import BeautifulSoup
reload(sys)
sys.setdefaultencoding('utf-8')


def get_authors(soup):
    authors = []
    for author in soup.find_all("span", class_="person"):
        author = author.getText("|", strip=True)
        author = str(author).replace(',', '')
        if "|mail|" or "|mail" in author:
            author = str(author).replace('|mail|', '')
            author = str(author).replace('|mail', '')
        elif "|equal contributor|" or "|equal contributor" in author:
            author = str(author).replace('|equal contributor|', '')
            author = str(author).replace('|equal contributor', '')
        authors.append(author)
    return authors


def get_email(soup):
    for email in soup.find_all(class_="author_inner"):
        for emailAddress in email.find_all('a'):
            return emailAddress.string


def get_authors_json(soup, users, email_address):
    author_list = []
    if get_email(soup) is None:
        for user in users:
            author_dict = {}
            author_dict["full_name"] = user
            author_dict["email"] = None
            author_list.append(author_dict)
    else:
        main_email = email_address.find_parent("li")
        main_email_user = get_authors(main_email)
        for user in users:
            if main_email_user[0] == user:
                author_dict = {}
                author_dict["full_name"] = user
                author_dict["email"] = email_address
                author_list.append(author_dict)
            else:
                author_dict = {}
                author_dict["full_name"] = user
                author_dict["email"] = None
                author_list.append(author_dict)
    return author_list


def get_keywords(soup):
    keywords = []
    for keyword in soup.find_all(class_="flagText"):
        keyword = keyword.string
        keywords.append(str(keyword))
    return keywords


def get_figures(soup):
    figures = []
    for figure in soup.find_all(class_="img"):
        for link in figure.find_all('a'):
            figures.append("http://www.plosone.org" + link.get('href'))
    return figures


def get_abstract(soup):
    abstract = soup.find(class_="abstract").find("p").getText()
    abstract = " ".join(abstract.split())
    return abstract


def normalize(result, timestamp):
    result_soup = BeautifulSoup(result)

    payload = {
        "doc":
            json.dumps({
                'title': result_soup.title.string,
                'contributors': get_authors_json(result_soup, get_authors(result_soup), get_email(result_soup)),
                'properties': {
                    'abstract': get_abstract(result_soup),
                    'tags': get_keywords(result_soup),
                    'figures': get_figures(result_soup),
                    'PDF': result_soup.find("meta", {"name": "citation_pdf_url"})['content']
                },
                'meta': {},
                'id': result_soup.find("meta", {"name": "citation_doi"})['content'],
                'source': "PLoS"
            }),
        'timestamp': timestamp
    }
    return requests.get('http://0.0.0.0:1337/process', params=payload)
