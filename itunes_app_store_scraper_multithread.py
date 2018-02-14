import string
import urllib.request
from urllib.parse import urlparse, parse_qs
from bs4 import BeautifulSoup
import sys
import time
import csv
from random import shuffle
import threading

from pymongo import MongoClient
from comments import get_comments
from pools import get_proxy, get_user_agent

# --------User Set These Global Variables----------#

# Set operation to "store" in order to get the initial list of links to scrape
# Set operation to "apps" in order to get the information for those links

operation = "apps"

# Set sleep time for the number of seconds between site requests (be polite!)
sleep = 0

# Set if you want to skip crawling existing
skip_existing = False

# Set if you want to remove link from links database when insert data on apps database
remove_inserted = False

# Set if you want to use proxy from proxy pool to request
use_proxy = False

# Number of threads spawned to call the itunes store at one time. Must be an integer.
threads = 2

# Number of retries in case of unsuccessful attempt
retries = 5

# Maximum number of pages that we navigate for each letter in each genre
num_of_letter_pages = 5

# --------DO NOT TOUCH!----------#
nav_site = "http://itunes.apple.com/us/genre/ios-books/id6018?mt=8"
alphabet = ['#', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T',
            'U', 'V', 'W', 'X', 'Y', 'Z']


# --------Program Functions----------#
def site_open(site):
    '''Makes connection and opens up target website. Returns a website object.'''
    try:
        # sets up request object
        req = urllib.request.Request(site)

        # adds User-Agent info to request object
        req.add_header("User-Agent", get_user_agent())
        # adds proxy to request object
        if use_proxy:
            proxy = urllib.request.ProxyHandler({'HTTP': get_proxy()})
            opener = urllib.request.build_opener(proxy)
            urllib.request.install_opener(opener)
        # opens up site
        website = urllib.request.urlopen(req)

        return website
    except urllib.request.URLError:
        print('Could not connect to ' + site + '!')
        pass


def dict_get(soup):
    dic = {}
    page_title = title_get(soup)
    try:
        dic['description'] = description_get(soup)
    except:
        print("Description is missing in the " + page_title)
    try:
        dic['metadata'] = info_get(soup)
    except:
        print("metadata is missing in the " + page_title)
    try:
        dic['rating'] = rating_get(soup)
    except:
        print("Rating is missing in the " + page_title)
    try:
        dic['title'] = title_get(soup)
    except:
        print("Title is missing in the " + page_title)
    try:
        dic['versions'] = versions_get(soup)
    except:
        print("Version history is missing in the " + page_title)

    return dic


def soup_site(site):
    '''opens site and turns it into a format to easily parse the DOM. Returns a Soup Object'''
    return BeautifulSoup(site_open(site))


def description_get(soup):
    return soup.find("div", {"class": "section__description"}).find("p").get("aria-label")


def whatsnew_get(soup):  # new
    return soup.find("div", "center-stack").find_all("p")[1].text


def versions_get(soup):
    versions = {}
    history_items = soup.find("ul", {"class": "version-history__items"}).find_all("li",
                                                                                  {"class": "version-history__item"})
    for item in history_items:
        version_number = item.find("h4", {"class": "version-history__item__version-number"}).text
        versions[version_number] = {}
        versions[version_number]['date'] = item.find("time", {"class": "version-history__item__release-date"}).get(
            "aria-label")
        versions[version_number]['note'] = item.find("div", {"class": "version-history__item__release-notes"}).get(
            "aria-label")
    return versions


def info_get(soup):  # new
    informations = {}
    information_list = soup.find("dl", {"class": "information-list"}).findAll("div",
                                                                              {"class": "information-list__item"})
    for item in information_list:
        informations[item.find("dt").text] = item.find("dd").text.replace('\n', '').replace("            ", '').replace(
            "          ", '')
    return informations


def copyright_get(soup):  # new
    return soup.find(id="left-stack").find("ul").find("li", "copyright").text


def price_get(soup):
    '''Returns price in $'''
    # price is the text in the first <li> in the "left-stack" <div>
    return soup.find(id="left-stack").find("ul").find("li").text


def title_get(soup):
    '''Returns App Title Text'''
    # title is the text in the <h1> tag in the "title" <div>
    return soup.find("h1", {"class": "product-header__title"}).text.split('\n')[1][10:]


def dev_get(soup):
    '''Returns developer name text'''
    # dev name is the text in the <h2> tag in the "title" <div>
    return soup.find(id="title").find("h2").text[3:]


def rating_get(soup):
    '''Returns tuple (# of Stars, # of Ratings)'''

    # rating is located in the "aria-label" tag in the <div class="rating">
    # in the <div class="customer-ratings>
    avg = soup.find("div", {"class": "we-customer-ratings__stats"}).find("span", {
        "class": "we-customer-ratings__averages__display"}).text
    count = soup.find("h4", {"class": "we-customer-ratings__count"}).text.split(' ')[0]
    star_row = soup.findAll("div", {"class": "we-star-bar-graph__row"})
    stars = {}
    for i in range(5, 0, -1):
        stars[str(i)] = \
        star_row[5 - i].find("div", {"class": "we-star-bar-graph__bar__foreground-bar"}).get("style").split(' ')[1][:-1]
    return {'average': avg, 'count': count, 'stars': stars}


def compatibility_get(soup):
    return soup.find(id="left-stack").find("p").find_all("span")[1].text


def genre_link_list(site):
    '''Generator function that 
    outputs each genre in the app store when called
    to be used when getting general App Store Links.
    Yields a genre url.'''

    # open site and makes navigatable DOM
    soup = soup_site(site)

    # creates array of genre links
    table = soup.find(id="genre-nav").find_all("a")

    for link in table:
        # returns next link when called
        yield link.get('href')


def app_link_list(site):
    '''Generator function that 
    outputs a single specific app link in the app store when called
    to be used when getting general App Store Links.
    Yields an app url.'''

    # open site and makes navigatable DOM
    soup = soup_site(site)

    # makes table of app links on a specific page
    table = soup.find(id="selectedcontent").find_all("a")
    for link in table:
        # outputs tuple with the link that will be written to a csv file
        yield (link.get('href'),)


def insert_to_links_database(document, collection):
    collection.update({"_id": document['_id']}, document, upsert=True)


def general_app_store_crawl(collection, sleep_time=float):
    '''Creates list of iTunes app store links.
    Grabs the first page of links for the first letter of the alphabet
    for every genre and writes it to a csv file.

    Inputs:
    1) file_name is the name of the output file_name
    2) sleep_time is a float value that indicates pause between site requests.

    Output: Mongodb :lists most US iTunes store apps'''

    # grabs everything from the database
    for i, link in enumerate(genre_link_list(nav_site)):

        print('Scraping genre ' + str(i) + '.')
        # loops through the alphabet to generate relevant sites
        # and insert to mongodb
        for letter in alphabet:
            for page_number in range(1, num_of_letter_pages + 1):
                # reconstructs URL
                new_site = link + "&letter=" + letter + "&page=" + str(page_number)
                print('Scraping from ' + new_site + '.')

                # pause for politeness
                time.sleep(sleep_time)

                # iteratively calls the app_link_list() function
                for app in app_link_list(new_site):
                    # make document for link database
                    link_document = {
                        "_id": app[0].split('/')[-1][2:-5],
                        "address": app[0],
                        "genre": str(link).split('/')[-2]
                    }
                    # add document to database
                    insert_to_links_database(link_document, collection)
    print('Completed Scrapping')
    return


def read_in_from_links_database(collection):
    '''reads in app store links from the app links file.
    Returns array.'''
    data = []
    for document in collection.find():
        data.append(document)
    shuffle(data)
    return data


def split_data(data, splits):
    '''Returns an array of arrays [[data1],[data2],...]
    based on the number of threads the user designates.
    This is used for multithreading purposes'''
    n = round(len(data) / splits)
    print("Total links: " + str(len(data)))
    for item in data:
        item["attempts"] = 0
    new_data = []
    for i in range(splits):
        j = data[i * n:(i + 1) * n]
        new_data.append(j)
    return new_data


def app_info_crawl(source, output, sleep_time=float, sample_size=None, num_threads=1):
    '''Takes in Source URLs and outputs App Info
    Inputs:
    1) source is the name of the input file with the list of urls to crawl
    2) output is the name of the output file_name with full app information
    3) sleep_time is a float value that indicates pause between site requests.
    4) num_threads is the number of threads that the script spawns to speed up process.

    Returns X number of csv files with app info, where X is equal to the number
    of threads used to grab app info. You can merge them all together in the command line
    using the command "cat *.debug
    csv > output_file"'''

    # reads in list of apps from database
    data = read_in_from_links_database(source)

    # breaks up data into an array of smaller data arrays
    # for multithreading purposes
    data = split_data(data, num_threads)

    # into the output csv
    for i in range(len(data)):
        link_list = data[i]
        # spawns thread and starts scrapping
        t = threading.Thread(target=app_crawl_main_loop, args=(output, link_list, i))
        t.start()
    print('Completed Spawning Threads')
    return


def insert_to_apps_database(collection, document):
    if exists_in_apps_database(collection, document.get('_id')):
        old = collection.find_one({"_id": document.get('_id')})
        # merge elder comments with new ones
        old_comments = old.get('comments')
        old_comments_size = len(old_comments)
        old_comments.update(document.get('comments'))
        # merge elder versions with new ones
        old_versions = old.get('versions')
        old_versions.update(document.get('versions'))

        collection.update({"_id": document['_id']}, {
            "$set": {"comments": old_comments, "versions": old_versions, "metadata": document.get("metadata")}},
                          upsert=True)
        print(str(old.get('_id')) + "'s comments are updated. Count: " + str(old_comments_size) + "->" + str(
            len(old_comments)))
    else:
        collection.update({"_id": document.get('_id')}, document, upsert=True)


def exists_in_apps_database(collection, id):
    if collection.count({'_id': id}) > 0:
        return True
    return False


def app_crawl_main_loop(collection, data, thread_id):
    '''Called by a thread in app_info_crawl(). Loops through
    a sub-data array and writes output to a sub-csv file.'''
    while (len(data) > 0):
        for link_document in data:
            link_document["attempts"] += 1
            link = link_document.get("address")
            print("Thread #" + str(thread_id) + ": Scrapping " + str(link) + ".")
            try:
                # get a dictionary from app page to save in database
                info_document = dict_get(soup_site(link))
                print(info_document)
                # check whether data was inserted before
                if exists_in_apps_database(collection, link.split('/')[-1][2:-5]) and skip_existing:
                    print(str(link.split('/')[-1][2:-5]) + " skipped because of duplication")
                    continue
                info_document['_id'] = link.split('/')[-1][2:-5]
                info_document['link'] = link
                info_document['comments'] = get_comments(link.split('/')[-1][2:-5])
                # opens the site, parses out the data, and writes it into the csv
                insert_to_apps_database(collection, info_document)
                data.remove(link_document)
            except:
                # continues loop of error is found and skips entry
                print("Could not add " + str(link) + " to data. Attempts: " + str(link_document["attempts"]))
                if link_document["attempts"] == retries:
                    data.remove(link_document)
                continue
    print('Completed Scrapping Data')
    return


def main():
    '''Main function that runs either the general app store crawler
    or the individual app crawler, depending on what "operation" is set to
    at the head of the script.'''
    print("Waiting for mongodb connection")
    client = MongoClient()
    db = client.appstore
    print("MongoClient connected successfully")

    # runs the main store crawl and prints out total time spent
    if operation == 'store':
        links = db.links
        start_time = time.time()
        general_app_store_crawl(links, sleep)
        print(time.time() - start_time)

    # runs the app info crawl and prints out total time spent
    elif operation == 'apps':
        apps = db.apps
        links = db.links
        start_time = time.time()
        selected_links = [{
                              "address": "https://itunes.apple.com/us/app/butt-sworkit-free-workout-trainer-to-tone-lift/id1000708019?mt=8"},
                          {
                              "address": "https://itunes.apple.com/us/app/deliveroo-restaurant-delivery-order-food-nearby/id1001501844?mt=8"}]
        app_info_crawl(links, apps, sleep, 0, threads)
        print(time.time() - start_time)
    else:
        print('You need to set "operation" to "store" or "apps"!')
    return


if __name__ == '__main__':
    main()
