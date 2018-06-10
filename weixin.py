    # 以下为Windows/Mac/Linux/可在麻瓜编程在线环境运行的代码：
    from wxpy import *
    import time
    
    # 建议使用小号建群，运行代码时用小号扫码登录，避免大号被封。如何购买小号请百度「微信小号」
    # 运行代码之前需要先建好群，修改群名称和代码中一致，至少在群里说一句话，然后用大号加小号管理员即可自动入群
    
    bot = Bot()
    
    def listen(pwd):
        time.sleep(3)
        return [msg for msg in bot.messages if msg.text ==pwd]
    
    def add(users,group):
        try:
            group.add_members(users,use_invitation=True)
            return group
        except ResponseError:
            return None
    
    def get_newfren(say):
        time.sleep(3)
        return [msg for msg in bot.messages if msg.text==say]
    
    group = bot.groups().search('test group 1')[0]
    
    while True:
        new_fren = get_newfren('Make friend')
        if new_fren:
            print('Found new one!')
            for msg in new_fren:
                new_user = msg.card
                bot.accept_friend(new_user)
                new_user.send('Hi new friend')
                bot.messages.remove(msg)
    
        time.sleep(3)
    
        print('Running')
        selected = listen('Pull me in')
        if selected:
            print('Found a new friend!XD')
            for msg in selected:
                this_user = msg.sender
                add(this_user,group)
                bot.messages.remove(msg)


