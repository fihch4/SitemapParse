#!/usr/bin/python3.8
from bs4 import BeautifulSoup
import time
import re
import requests
import urllib3

import datetime
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
from config import *
from apache_mysql import MySQLi
db = MySQLi(host, user, password, database_home)
import urllib3
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings((InsecureRequestWarning))
def get_sitemaps(sitemap_url):
    try:
        request = requests.get(sitemap_url, headers=headers, verify=False)
        return request.text
    except:
        print("Ошибка парсинга основной карты сайта")


def get_list_sitemaps(request_sitemap_url):
    try:
        sitemap_links = []
        if '.xml</loc>' in request_sitemap_url:
            soup = BeautifulSoup(request_sitemap_url, 'xml')
            for url in soup.find_all('loc'):
                sitemap_links.append(url.text)
            return sitemap_links
    except:
        print("Ошибка парсинга карты сайтов")


def list_urls(list_sitemaps):
    links_in_sitemap = []
    try:
        for url in list_sitemaps:
            products_in_sitemap = get_sitemaps(url)
            soup = BeautifulSoup(products_in_sitemap, 'xml')
            for url_product in soup.find_all('loc'):
                links_in_sitemap.append(url_product.text)
            print(f"Всего URL в списке: {len(links_in_sitemap)}")
            time.sleep(5)
        return links_in_sitemap

    except:
        print("Ошибка парсинга товаров из карт сайта")


def list_urls_from_one_sitemap(sitemap):
    links_in_sitemap = []
    soup = BeautifulSoup(sitemap, 'xml')
    for url_product in soup.find_all('loc'):
        links_in_sitemap.append(url_product.text)
    print(f"Всего URL в списке: {len(links_in_sitemap)}")
    time.sleep(5)
    return links_in_sitemap


def main():
    date = datetime.datetime.utcnow().date()
    dom_sitemaps = get_sitemaps(sitemap_url)
    sitemaps = get_list_sitemaps(dom_sitemaps)
    for_domain_id = db.fetch("SELECT id FROM a_team_webmaster_domains WHERE domain LIKE %s", ("%" + host_db + "%"))
    domain_id = for_domain_id['rows'][0][0]

    if sitemaps is not None:
        list = list_urls(sitemaps)
        for url in list:
            check_string_in_db = db.fetch("select url FROM a_team_sitemaps WHERE url= %s AND datetime = %s", url, date)
            if not check_string_in_db['rows']:
                # print(f"URL - {url} - Переменная rows {check_string_in_db['rows']} - ! Записали в БД")
                db.commit("INSERT INTO a_team_sitemaps (host, url, datetime, id_domain) VALUES (%s, %s, %s, %s)",
                          host_db, url, date, domain_id)
    else:
        list = list_urls_from_one_sitemap(dom_sitemaps)
        for url in list:
            check_string_in_db = db.fetch("select url FROM a_team_sitemaps WHERE url= %s AND datetime = %s", url, date)
            if not check_string_in_db['rows']:
                # print(f"URL - {url} - Переменная rows {check_string_in_db['rows']} - ! Записали в БД")
                db.commit("INSERT INTO a_team_sitemaps (host, url, datetime, id_domain) VALUES (%s, %s, %s, %s)",
                          host_db, url, date, domain_id)
    print("Парсинг карты сайта завершен")


if __name__ == '__main__':
    main()
