from datetime import datetime

import feedparser

def read_in_from_apps_database(collection):
    '''reads in app store links from the app links file.
    Returns array.'''
    data=[]
    for document in collection.find():
        data.append(document)
    return data

def get_comments(id):
    comments_dic = {}
    for page in range(1, 11):
        # print("Page " + str(page) + ":")
        feedUrl = "https://itunes.apple.com/us/rss/customerreviews/id=" + str(id) + "/page=" + str(page) + "/sortBy=mostRecent/xml"
        # Connect to the site and download the feed data:
        feed = feedparser.parse(feedUrl)
        # get the data for each entry (skipping first entry in each page which is dummy)
        for post in feed.entries[1:]:
            comment_id = post.get('id').split('/')[-1]
            title = post.title
            comment_text = post.content[0]['value']
            time = post.get('updated')
            rating = post.get('im_rating')
            version = post.get('im_version')
            votecount = post.get('im_votecount')
            votesum = post.get('im_votesum')
            author = post.get('author_detail')
            comments_dic[comment_id] = {'title': title, 'text': comment_text, 'time': time,'version': version, 'rate': rating, 'plus_vote': votesum, 'vote_count': votecount, 'author': author}
    return comments_dic