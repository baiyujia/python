# Windows版的代码请在这下载：https://video.mugglecode.com/net4.py
# 以下为Mac/Linux/可在麻瓜编程在线运行的代码：
from selenium import webdriver
import time

# 运行前先下载 chrome driver,下载地址是：https://sites.google.com/a/chromium.org/chromedriver/downloads，点击【Latest Release: ChromeDriver x.xx】进入下载

url = 'https://www.wenjuan.com/s/eqYVbug/?skip_grant=1#' #可以替换成你想跟踪的单条微博链接
def start_chrome():
    driver = webdriver.Chrome(executable_path='/usr/local/bin/chromedriver')  # Windows 需写成'./chromedriver.exe'
    driver.start_client()
    return driver

def find_info(driver):
    # css_selector
    sel   = '#question_59abdb0492beb51b7007c144 > div.topic_title'
    elems = driver.find_elements_by_css_selector(sel)
    return [int(el.text) for el in elems[1:]]

def main():
    driver = start_chrome()
    input('请修改user agent:')
    driver.get(url)
    time.sleep(6) # wait loading
    find_info(driver)
    print('Done!')
if __name__ == '__main__':
    main()