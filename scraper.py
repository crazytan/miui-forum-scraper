# -*- coding: utf-8 -*-
"""
小米论坛爬虫
@author Jia Tan
"""

import io
import requests
import re
import threading
from datetime import datetime

sub_list = [('266', u'相册')]

reply_type = [u'已收录', u'已答复', u'请补充', u'待讨论', u'确认解决']

# regex to remove whitespaces`
white_re = re.compile(r'\s+', re.U)

# regex to get page count
page_cnt_re = re.compile(r'class=\"last\">... ([0-9]+)', re.U)

# regex to get thread content
thread_re = re.compile(r'<divclass=\"avatarbox-info\">'
                       r'<divclass=\"sub-tit\">(.+?)</div>'
                       r'<divclass="sub-infos">(.+?)</div></div>', re.U)

# regex to get name of subsection
sub_name_re = re.compile(r'<em>.+?>(.+?)</a>\]</em>', re.U)

# regex to get thread title
thread_title_re = re.compile(r'class=\"sxst\">(.+?)</a>', re.U)

# regex to get view count
view_re = re.compile(r'<spanclass=\"number_d\">(\d+)</span>', re.U)

# regex to get reply count
reply_re = re.compile(r'<spanclass=\"number_d\"><ahref=.+?(\d+)</a></span>', re.U)

# regex to get url for thread page
thread_page_re = re.compile(r'</em><ahref=\"(.+?)\"onclick', re.U)

# regex to get post time for thread
date_time_re = re.compile(ur'发表于 <span title=\"(\d+-\d+-\d+ \d+:\d+:\d+)\">|发表于 (\d+-\d+-\d+ \d+:\d+:\d+)</em>', re.U)

# global thread content queue
thread_queue = []

# global lock for thread queue
queue_lock = threading.Lock()


def get_content(url):
    res = requests.get(url)
    while res.status_code != 200:
        if res.status_code == 404:
            print '404 not found: ' + url
            return ''
        res = requests.get(url)
    return res.text


def grep(s, *args):
    ret = []
    for r in args:
        ret.append(re.findall(r, s)[0])
    return ret


def get_reply(s):
    for r in reply_type:
        if s.find(r) >= 0:
            return r
    return u'无回复'


def produce():
    for sub_str, sub_name in sub_list:
        first_page = get_content('http://www.miui.com/type-38-' + sub_str + '.html')
        if not first_page:
            continue
        page_cnt = int(re.search(page_cnt_re, first_page).group(1))
        print '%d pages for %s' % (page_cnt, sub_name)
        print 'page,'
        for page_num in range(1, page_cnt + 1):
            print '%d ' % page_num,
            page_url = 'http://www.miui.com/forum.php?mod=forumdisplay&fid=38&typeid=' \
                       + sub_str + '&filter=typeid&page=' + str(page_num)
            page = get_content(page_url)
            with queue_lock:
                thread_queue.append(page)
        print ''
    with queue_lock:
        thread_queue.append('')


def consume(fout):
    while True:
        page = None
        queue_lock.acquire()
        if len(thread_queue) > 0:
            page = thread_queue.pop(0)
        queue_lock.release()
        if page is None:
            continue
        if not page:
            break
        page = re.sub(white_re, '', page)
        threads = re.findall(thread_re, page)
        for thread in threads:
            sub_name, thread_title, thread_url = grep(thread[0], sub_name_re, thread_title_re, thread_page_re)
            if_attach = u'是' if thread[0].find(u'附件') >= 0 else u'否'
            if_extra_points = u'是' if thread[0].find(u'加分') >= 0 else u'否'
            reply = get_reply(thread[0])
            thread_content = get_content('http://www.miui.com/' + thread_url)
            try:
                date_time_match = re.search(date_time_re, thread_content).groups()
            except AttributeError as e:
                with io.open('error_page.html', 'w', encoding='utf-8') as ferr:
                    ferr.write(thread_content)
                print '\nhttp://www.miui.com/' + thread_url
            date_time = date_time_match[0] if date_time_match[0] else date_time_match[1]
            view_num, reply_num = grep(thread[1], view_re, reply_re)
            fout.write(','.join([date_time, sub_name, view_num, reply_num, reply,
                                 if_attach, if_extra_points, thread_title]) + '\n')
        fout.flush()
    fout.close()

if __name__ == "__main__":
    fout_name = 'miui_' + '_'.join(map(lambda x: x[-1], sub_list)) + '_' + str(datetime.now().date()) + '.csv'
    fout = io.open(fout_name, 'w', encoding='utf-8')
    fout.write(u'发布日期,分类,浏览数,回复数,小米回复类型,是否有附件,是否被加分,标题\n')
    producer = threading.Thread(target=produce)
    consumer = threading.Thread(target=consume, args=(fout,))
    producer.start()
    consumer.start()
