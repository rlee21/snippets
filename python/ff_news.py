#!venv/bin/python

from requests_html import HTMLSession


def get_news(url):
    """ fetch 10 latest players news """
    session = HTMLSession()
    r = session.get(url)
    r.html.render()
    for i in range(1, 11):
        css_selector = "#player-news-page-wrapper > div > div > div.player-news.default > ul > li:nth-child({index}) > article > div.player-news-article__body > div.player-news-article__title > h3".format(index=i)
        headline = r.html.find(css_selector, first=True)
        try:
            print("{0}) {1}\n".format(i, headline.text))
        except AttributeError:
            pass


def main():
    URL = "https://www.rotoworld.com/football/nfl/player-news"
    get_news(URL)


if __name__ == '__main__':
    main()
