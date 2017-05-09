import praw
import config


def bot_login():
    print("Logging into reddit...") 
    reddit = praw.Reddit(username = config.username,
                         password = config.password,
                         client_id = config.client_id,
                         client_secret = config.client_secret,
                         user_agent = "rlee21's reddit bot v0.1")

    return reddit


def run_bot(reddit, v_subreddit):
    print("Obtaining 25 submissions for {0}...".format(v_subreddit))
    submissions = reddit.subreddit(v_subreddit).hot(limit=25)
    for nbr, submission in enumerate(submissions):
        print(nbr + 1, submission.title)



if __name__ == "__main__":
    reddit = bot_login()
    v_subreddit = input("Enter subreddit: ")
    run_bot(reddit, v_subreddit)
