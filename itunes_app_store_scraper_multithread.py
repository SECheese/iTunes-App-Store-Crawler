import urllib.request
from urllib.parse import urlparse, parse_qs
from bs4 import BeautifulSoup
import sys
import time
import csv
from random import shuffle
import threading

from pymongo import MongoClient

# --------User Set These Global Variables----------#

# Set operation to "store" in order to get the initial list of links to scrape
# Set operation to "apps" in order to get the information for those links
from comments import get_comments

operation = "apps"

# Set sleep time for the number of seconds between site requests (be polite!)
sleep = 0

# Set sample to an integer (not in the quotes) for the number of apps to get info for
# out of the whole file

# Set sample to '' to crawl the whole data set at once
sample = ''

# Number of threads spawned to call the itunes store at one time. Must be an integer.
threads = 2

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
        req.add_header("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_8) \
         AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.46 Safari/536.5")

        # opens up site
        website = urllib.request.urlopen(req)

        return website
    except urllib.request.URLError:
        print('Could not connect to ' + site + '!')
        pass


def dict_get(soup):  # TODO make full dictionary of whole data including version and link(?)
    dic = {}
    try:
        dic['description'] = description_get(soup)
        dic['metadata'] = left_side_get(soup)
        dic['price'] = price_get(soup)
        dic['copyright'] = copyright_get(soup)
        dic['rating'] = rating_get(soup)
        dic['compatibility'] = compatibility_get(soup)
        dic['title'] = title_get(soup)
        dic['developer'] = dev_get(soup)

    except:
        print("Something is missing in the page.")
        pass

    return dic


def soup_site(site):
    '''opens site and turns it into a format to easily parse the DOM. Returns a Soup Object'''
    return BeautifulSoup(site_open(site))


def description_get(soup):  # new
    return soup.find("div", "center-stack").find("p").text


def left_side_get(soup):  # new
    left_side_dic = {}
    ls = soup.find(id="left-stack").find("ul").find_all("li")
    n = len(ls)

    for i in range(1, n):
        span = ls[i].find_all("span")
        span_size = len(span)

        if span_size == 1:
            key = ls[i].text.split(":")[0][:-2]
            value = ls[i].text.split(":")[1]
            left_side_dic[key] = value

        elif span_size == 2:
            key = span[0].text[:-2]
            value = span[1].text
            left_side_dic[key] = value

    return left_side_dic


def copyright_get(soup):  # new
    return soup.find(id="left-stack").find("ul").find("li", "copyright").text


def price_get(soup):
    '''Returns price in $'''
    # price is the text in the first <li> in the "left-stack" <div>
    return soup.find(id="left-stack").find("ul").find("li").text


def title_get(soup):
    '''Returns App Title Text'''
    # title is the text in the <h1> tag in the "title" <div>
    return soup.find(id="title").find("h1").text


def dev_get(soup):
    '''Returns developer name text'''
    # dev name is the text in the <h2> tag in the "title" <div>
    return soup.find(id="title").find("h2").text[3:]

def rating_get(soup):
    '''Returns tuple (# of Stars, # of Ratings)'''

    # rating is located in the "aria-label" tag in the <div class="rating">
    # in the <div class="customer-ratings>
    tag = soup.find("div", "customer-ratings").find("div", "rating")

    # splits array of stars and rating from the "aria-label" tag
    stars, rating = tag['aria-label'].split(',')

    temp1 = soup.find("div", "app-rating").find("a").text

    temp2 = ""
    if soup.find("div", "app-rating").find("ul") is not None:
        temp2 = soup.find("div", "app-rating").find("ul").text

    # return a tuple of stars and rating without whitespace
    return (stars.strip(), rating.strip()), temp1, temp2


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

            # reconstructs URL
            new_site = link + "&" + letter
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
    return data


def split_data(data, splits):
    '''Returns an array of arrays [[data1],[data2],...]
    based on the number of threads the user designates.
    This is used for multithreading purposes'''
    n = round(len(data) / splits)
    print(len(data))
    new_data = []
    for i in range(0, splits):
        j = data[(i - 1) * n:i * n]
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
    for link_list in data:
        # spawns thread and starts scrapping
        t = threading.Thread(target=app_crawl_main_loop, args=(output, link_list))
        t.start()
    print('Completed Spawning Threads')
    return


def insert_to_apps_database(collection, document):  # TODO add update part to support adding new versions
    collection.update({"_id": document['_id']}, document, upsert=True)


def app_crawl_main_loop(collection, data):
    '''Called by a thread in app_info_crawl(). Loops through
    a sub-data array and writes output to a sub-csv file.'''
    for link_document in data:
        link = link_document.get("address")
        print("Scrapping #" + str(link) + ".")
        try:
            # get a dictionary from app page to save in database
            info_document = dict_get(soup_site(link))
            info_document['_id'] = link.split('/')[-1][2:-5]
            info_document['link'] = link
            info_document['comments'] = get_comments(link.split('/')[-1][2:-5])
            # opens the site, parses out the data, and writes it into the csv
            insert_to_apps_database(collection, info_document)
        except:
            # continues loop of error is found and skips entry
            print("Could not write to csv file for some reason =0")
            continue
    print('Completed Scrapping Data')
    return


def main():
    # print(description_get(soup_site("https://itunes.apple.com/us/app/kindle-read-ebooks-magazines-textbooks/id302584613?mt=8&ign-mpt=uo%3D2")))

    print(dict_get(soup_site("https://itunes.apple.com/us/app/trvl/id391961927?mt=8%22")))

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
        app_info_crawl(links, apps, sleep, sample, threads)
        print(time.time() - start_time)
    else:
        print('You need to set "operation" to "store" or "apps"!')
    return


if __name__ == '__main__':
    main()




