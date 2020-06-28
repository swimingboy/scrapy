'''
时间表提取
'''
from bs4 import BeautifulSoup
import bs4
import re
from time import sleep, localtime
import os
from datetime import datetime, timedelta
from typing import List


class Period:
    start: datetime = None
    end: datetime = None

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        if self.start:
            start = self.start.strftime('%Y/%m/%d %H:%M')
        else:
            start = ''
        if self.end:
            end = self.end.strftime('%Y/%m/%d %H:%M')
        else:
            end = ''
        return start + '-' + end


class TimesheetExtracter:

    @staticmethod
    def print_tree(elem, spec=0):
        if elem.name:
            print('-' * spec, elem.name)
        if hasattr(elem, 'children'):
            for i in elem.children:
                TimesheetExtracter.print_tree(i, spec + 2)

    @staticmethod
    def izip(*iters):
        for iter_ in iters:
            if iter_:
                for i in iter_:
                    yield i

    @staticmethod
    def get_bro_by_parents(elem, Tag=False, content_self=False):
        try:
            parents = next(elem.parents)
        except:
            return []
        ret = []
        for i in parents.children:
            if not content_self and i is elem:
                continue
            if type(i) == bs4.element.Comment:
                continue

            if Tag and type(i) != bs4.element.Tag:
                continue
            ret.append(i)
        return ret

    @staticmethod
    def unwrap_all(elem, spec=0, show_debug=False):
        '''
        如果是根节点, 那么直接使用其body
        如果元素是Tag:
            对于所有的dt, dd, td, th, 都将其子元素展开为纯文本
            如果元素只有一个子Tag元素, 子元素成为该元素父元素的子元素, 销毁此元素
            如果元素只有文本元素, 
                如果它没有兄弟元素, 或者(兄弟元素只有一个, 并且兄弟元素可以解析为日期)
                    文本成为父元素的子元素, 销毁此元素
            
        Tag , NavigableString , BeautifulSoup , Comment
        '''
        elem_type = type(elem)
        # print(show_debug)
        if elem_type == bs4.BeautifulSoup:
            TimesheetExtracter.unwrap_all(elem.body, show_debug=show_debug)

        if elem_type == bs4.element.Tag:
            show_debug and print('elem: ', elem)
            if elem.name in ['dt', 'dd', 'td', 'th']:
                elem.string = elem.text

            children = [i for i in elem.children if type(i) == bs4.element.Tag]
            parents = next(elem.parents)
            if len(children) == 1 and type(parents) != bs4.BeautifulSoup:
                show_debug and print('one children: ', children[0].name)
                elem.unwrap()
                elem = parents
                show_debug and print('result elem: ', elem)
            children_type = [type(i) in [bs4.element.NavigableString, bs4.element.Comment] for i in elem.children]

            if all(children_type):
                bros = TimesheetExtracter.get_bro_by_parents(elem, Tag=False, content_self=False)
                flag = False
                if len([i for i in bros if type(i) == bs4.element.Tag]) == 0:
                    flag = True
                elif all([type(i) != bs4.element.Tag for i in bros]):
                    # print("".join([i.text for i in bros]))

                    ret = TimesheetExtracter.extract_date("".join([i.text for i in bros]))
                    if ret['msg'] == 'ok':
                        flag = True

                if flag:
                    show_debug and print('text children: ', elem.name)
                    elem.unwrap()
                    elem = parents
                    show_debug and print('result elem: ', elem)
        if hasattr(elem, 'children'):
            for i in elem.children:
                TimesheetExtracter.unwrap_all(i, spec + 2, show_debug=show_debug)

    @staticmethod
    def flat_tree(elem, layer=0, nodes=None):

        if not nodes:
            nodes = []
        if len(nodes) <= layer:
            nodes.append([])
        if type(elem) == bs4.element.Tag:
            nodes[layer].append(elem)

        if not hasattr(elem, 'children'):

            return
        for i in elem.children:

            TimesheetExtracter.flat_tree(i, layer + 1, nodes)

        return nodes

    @staticmethod
    def build_date(year=None, month=None, day=None, hour=None, minute=None):
        now = datetime.now()
        if not year:
            year = now.year
        else:
            year = int(year)
        if not month:
            month = now.month
        else:
            month = int(month)
        if not day:
            day = now.day
        else:
            day = int(day)

        if not hour:
            hour = now.hour
        else:
            hour = int(hour)
        if not minute:
            minute = now.minute
        else:
            minute = int(minute)
        return datetime(year, month, day, hour, minute)

    @staticmethod
    def extract_date(text):
        text = text.strip()
        # print('extract_date:',text)
        for key_word, date in [
            ('本日', datetime.now().strftime("%m/%d")),
            ('今日', datetime.now().strftime("%m/%d")),
            ('明日', (datetime.now() + timedelta(days=1)).strftime("%m/%d")),
        ]:
            text = text.replace(key_word, date)
        weekday = '月火水木金土日'
        print('176:',text)
        if text in weekday:
            today_of_week = datetime.today().weekday()
            text_of_week_order = weekday.find(text)
            if text_of_week_order != -1:
                if today_of_week > text_of_week_order:
                    text_of_week_order += 7
                text = text.replace(
                    text,
                    (datetime.now() + timedelta(
                        days=text_of_week_order - today_of_week
                    )).strftime("%m/%d")
                )

        msg = ''
        datetime.today().weekday()
        res = re.findall(r'\d+', text)
        print(text)
        print('extract_date:192:',res)
        if len(res) == 2:
            mon, day = res
            msg = 'ok'
        elif len(res) > 2:
            mon, day, *left = res
            msg = 'warning'
            # print(f'warning date: {text}, {res}')
        # elif len(res) == 1:
        #     day, *left = res
        #     mon = datetime.now().date().month
        #     msg = 'warning'
        #     # print(f'warning date: {text}, {res}')
        elif len(res) == 0:
            mon, day = 0, 0
            msg = 'error'

        else:
            mon, day = 0, res[0]
            msg = 'error'
            print(f'error date: {text}, {res}')
        return dict(date=TimesheetExtracter.build_date(month=mon, day=day), msg=msg)

    @staticmethod
    def extract_time(text, show_debug=False):
        # 15:00～ラスト
        print(text)
        res = re.findall(r'\d{1,2}:\d{1,2}', text)
        if not res:
            res = re.findall(r'\d{1,2}', text)
            res = [i+':00' for i in res]
        print('e1',res)
        msg = ''
        if len(res) == 2:
            start, end = res
            msg = 'ok'
        elif len(res) > 2:
            start, end, *left = res
            msg = 'warning'
            show_debug and print(f'warning date: {text}, {res}')
        elif len(res) == 1:
            msg = 'ok'
            if any([i in text.lower() for i in ['ラスト', 'last', '終了', '閉店', '最後']]):
                end = None
                start = res[0]
            elif any([i in text.lower() for i in ['オープン', 'open', '開始', '開店', '最初']]):
                start = None
                end = res[0]
            else:
                start = res[0]
                end = None
                msg = 'error'
        else:
            # start, end = ['00:00', '00:00']
            start, end = None, None

            msg = 'error'
            show_debug and print(f'error date: {text}, {res}')
        return dict(start=start, end=end, msg=msg)

    @staticmethod
    def clear_diff(layer):
        tags = [i.name for i in layer]
        diffs = [sum([tag_b == tag_a for tag_b in tags]) for tag_a in tags]
        for index, diff in enumerate(diffs):
            if diff == 1:
                # print(tags, diffs)
                lentags = len(tags)
                # print([diff_ for diff_ in diffs[:index] + diffs[index+1:]])
                other_diff = sum(
                    [diff_ == lentags - 1 for diff_ in diffs[:index] + diffs[index + 1:]]
                )
                if other_diff == lentags - 1:
                    # print('decompose: ', layer[index])
                    layer[index].decompose()
                    break

    @staticmethod
    def get_time_sheet(html: str, show_debug=False, time=None):
        for t in re.findall('(<br.*?>)', html):
            html = html.replace(t, ' ')
        bs = BeautifulSoup(html, 'html.parser')
        show_debug and TimesheetExtracter.print_tree(bs, 0)
        TimesheetExtracter.unwrap_all(bs, 0, show_debug)
        show_debug and TimesheetExtracter.print_tree(bs, 0)

        periods: List[Period] = []
        # print('277:',bs.text)
        flat = TimesheetExtracter.flat_tree(bs)
        # print('279:',flat)
        for layer in flat:
            # print(layer)
            if len(layer) >= 7:  # 7 days per week
                TimesheetExtracter.clear_diff(layer)
        # flat = TimesheetExtracter.flat_tree(bs)
        # print('f1',flat)
        # <这里为www.y-momo.jp加一个特例
        # 并为同类型的y-aqua.jp添加特例
        # if html.find('<ul class="clearfix">\n\n<li><span>') != -1 or \
        #         re.findall(r'）</span>\n\n<!---->\n\d{2}[:]\d{2}\xa0〜\xa0', html):
        #     temp_bs = BeautifulSoup(html, 'html.parser')
        #     lis = temp_bs.find_all('li')
        #     lens = [len(i) for i in flat]
        #     max_index = lens.index(max(lens))
        #
        #     for index, lii in enumerate(lis):
        #         t = BeautifulSoup("<div>" + list(lii.children)[-1] + "</div>", 'html.parser')
        #         flat[max_index].insert(2 * index + 1, t)
        # />

        lens = [len(i) for i in flat]
        max_index = lens.index(max(lens))
        max_value = lens[max_index]
        pre_value = lens[max_index - 1]
        print(lens,max_index,max_value,pre_value)

        try:
            if  max_value / pre_value in [7, 8,9]:
                # 若干排 七八列的, 日期是表头
                print('1')
                k = int(max_value / pre_value)
                elems = flat[max_index]
                # print('1k',elems[:k])
                # print('k:',elems[k:])
                date = [TimesheetExtracter.extract_date(i.text) for i in elems[:k]]

                # if all([i['msg'] == 'error' for i in date]):
                #     days = [i['date'].day for i in date]
                #     max_day = max(days)
                #     days = [i + max_day if i < 14 else i for i in days]
                #     if all([y - x == 1 for x, y in zip(days, days[1:])]):
                #         start_datetime = datetime.now()
                #         if days[0] == start_datetime.day:
                #             for i in date:
                #                 i['date'] = start_datetime
                #                 start_datetime += timedelta(days=1)
                #                 i['msg'] = 'ok'
                #     else:
                #         raise Exception('no date')

                time = [
                    [TimesheetExtracter.extract_time(i.text) for i in elems[k:2 * k]],
                    [TimesheetExtracter.extract_time(i.text) for i in elems[2 * k:3 * k]]
                ]


            elif max_value / pre_value == 2 or max_value in [14, 16, 28]:
                # 若干排 两列的, 一列日期, 一列时间
                print('two line')
                elems = flat[max_index]
                date = []
                time = [[]]
                index = 0
                # print(flat[max_index - 1])
                # print(elems[0], elems[1], elems[2], elems[3])
                for item in elems:
                    print('item:',item)
                    if index % 2 == 0:
                        extr = TimesheetExtracter.extract_date(item.text)
                        # print('data', extr)
                        date.append(extr)
                    else:
                        print('time')
                        time[0].append(TimesheetExtracter.extract_time(item.text))
                    index += 1
                print('若干排 两列的')

            else:
                # 我也不知道多少, 随缘匹配吧

                date = []
                time = [[]]

                # print_tree(bs, 0)
                # print(elems)
                print('我也不知道多少, 随缘匹配吧')
            # print('s1',date)
            # if date[0]['msg'] == 'ok' and (
            #         all([i['msg'] == 'error' for i in date[1:]])
            #         or
            #         sum([i['msg'] == 'error' for i in date[1:]]) <= 3
            # ):
            #     # 如果日期的第一个OK, 但是后面的不太OK, 那么可能是
            #     # 1/1 2 3 4 5 6 7
            #     # 这样的格式, 那么可以递推试试
            #     print('s2')
            #     for i in range(len(date[1:])):
            #         date[i + 1]['date'] = date[0]['date'] + timedelta(days=i + 1)
            print('ssssss')
            print('381:', time)
            print('382:',date)
            if all([i['msg'] == 'error' for i in date]):
                # 如果日期的日如果全都是错的, 但是能够排成公差为1的数列, 那就算上
                days = [i['date'].day for i in date]
                print(days)
                max_day = max(days)
                days = [i + max_day if i < 14 else i for i in days]
                if all([y - x == 1 for x, y in zip(days, days[1:])]):
                    start_datetime = datetime.now()
                    if days[0] == start_datetime.day:
                        for i in date:
                            i['date'] = start_datetime
                            start_datetime += timedelta(days=1)
                            i['msg'] = 'ok'
                else:
                    raise Exception('no date')

            if date[0]['msg'] == 'warning' and (time == [[]] or time == [[], []]):
                # 如果日期里有多余的数据, 而且时间是空, 可以去日期里找找时间
                elems = flat[max_index]
                time = [
                    [TimesheetExtracter.extract_time(i.text) for i in elems],
                ]

            flag = False
            print('s2',time)
            for time_ in time:
                for index, item in enumerate(time_):
                    if item['msg'] == 'ok':
                        period = Period()
                        if item['start'] != None:
                            hour, minute = [int(i) for i in item['start'].split(':')]
                            period.start = date[index]['date']
                            period.start -= timedelta(hours=period.start.hour)
                            period.start += timedelta(hours=hour)
                            period.start -= timedelta(minutes=period.start.minute)
                            period.start += timedelta(minutes=minute)
                        else:
                            period.start = datetime(*localtime(0)[:6])
                        if item['end'] != None:
                            hour, minute = [int(i) for i in item['end'].split(':')]
                            period.end = date[index]['date']
                            period.end -= timedelta(hours=period.end.hour)
                            period.end += timedelta(hours=hour)
                            period.end -= timedelta(minutes=period.end.minute)
                            period.end += timedelta(minutes=minute)
                        else:
                            period.end = datetime(*localtime(0)[:6])
                        periods.append(period)
                        flag = True
                if flag:
                    break
            print(periods)

        except Exception as e:
            print(e)

        return periods


if __name__ == "__main__":
    for file in os.listdir('html')[2:3]:
        with open(r'html/' + file, encoding='utf-8') as f:
            html = f.read()
        print(TimesheetExtracter.get_time_sheet(html.split('-split-')[0], show_debug=True))
