import random


def get_proxy():
    '''Returns a proxy form proxy pool'''
    return random.choice(proxy_pool)


def get_user_agent():
    '''Returns a user-agent from user-agent pool'''
    return random.choice(user_agent_pool)


# --------YOU CAN ADD YOUR PROXIES HERE----------#
proxy_pool=["", "localhost"
]


# ---YOU CAN ADD YOUR USER-AGENT INFOS HERE------#
user_agent_pool = [
"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_8) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.46 Safari/536.5"
]