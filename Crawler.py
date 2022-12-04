from typing import List
from pathlib import Path
from bs4 import BeautifulSoup
import requests
import lxml
from urllib.parse import urljoin
import re


class Crawler:

    def __init__(self, url, crawDataPath):
        self.rootUrl = url
        self.crawDataPath = crawDataPath

    def scrapUrl(self, url, path):
        page = requests.get(url)
        soup = BeautifulSoup(page.content, 'lxml')
        postHeading = soup.find('h1', class_='firstHeading mw-first-heading').text
        postBody = []
        for body in soup.findAll('p'):
            postBody.append(body.text)
        postBody = ''.join(postBody).replace('\n', '')
        hyperlinkList = []
        if len(postBody.strip()) == 0:
            return hyperlinkList
        for link in soup.find_all('a'):
            hyperlinkList.append(urljoin(url, link.get('href')))
        hyperlinkList = list(
            filter(lambda x: "en.wikipedia.org/wiki" in x and not bool(re.match(".*((//).*(:)|#|[\(\)\[\]\{\}]).*", x)),
                   hyperlinkList))
        f = open(path, "w", encoding="utf-8")
        f.write(postHeading + "\n" + postBody + "\n")
        f.close()
        return hyperlinkList

    def getDirectorySizeFlag(self):
        pathToMeasure = Path(self.crawDataPath)
        size = sum(f.stat().st_size for f in pathToMeasure.glob('**/*') if f.is_file())
        if size >= 1024 * 1024 * 1024:
            return True
        else:
            return False

    def getUniqueNewLinks(self, links: list, activeLinks: list, processedLinks: set) -> List:
        links = set(links)
        newLinks = set({})
        for element in links:
            if element not in processedLinks and element not in activeLinks:
                newLinks.add(element)
        return list(newLinks)

    def crawl(self):

        activeLinkList = [self.rootUrl]
        processedList = set({})
        counter1, counter2 = 0, 0
        while True:
            counter1, counter2 = counter1 + 1, counter2 + 1
            # print(counter1)
            if counter2 > 1000 and self.getDirectorySizeFlag():
                counter2 = 0
                print("1GB Size reached")
                break
            currentUrl = activeLinkList.pop(0)
            # print(currentUrl)
            if currentUrl in processedList:
                continue
            try:
                retrievedLinks = self.scrapUrl(currentUrl, self.crawDataPath + currentUrl.split("/")[-1] + ".txt")
            except:
                continue
            newLinksSorted = self.getUniqueNewLinks(retrievedLinks, activeLinkList, processedList)
            activeLinkList.extend(newLinksSorted)


if __name__ == '__main__':
    startUrl = "https://en.wikipedia.org/wiki/Coronavirus"
    crawDataTarget = "Resources/CrawlData/"
    crawler = Crawler(startUrl, crawDataTarget)
    crawler.crawl()
