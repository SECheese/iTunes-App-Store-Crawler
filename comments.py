import feedparser

def get_comments(id):
    for page in range(1, 11):
        print("Page " + str(page) + ":")
        feedUrl = "https://itunes.apple.com/us/rss/customerreviews/id=" + str(id) + "/page=" + str(page) + "/sortBy=mostRecent/xml"
        # Connect to the site and download the feed data:
        feed = feedparser.parse(feedUrl)
        # get the data for each entry (skipping first entry in each page which is dummy)
        for post in feed.entries[1:]:
            title = post.title
            commentText = post.content[0]['value']
            time = post.get('updated')
            rating = post.get('im_rating')
            votecount = post.get('im_votecount')
            votesum = post.get('im_votesum')
            author = post.get('author_detail')
            print(commentText)

get_comments(302584613)
