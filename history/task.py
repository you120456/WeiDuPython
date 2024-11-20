import os
import warnings
import sys
import schedule

warnings.filterwarnings("ignore")  # 关闭警告


def every_friday_work():
    path = r'D:\自动化python代码存放'
    sys.path.append(path)
    print(1)
    # import ITweekReport


def every_month_work():
    path = r'D:\自动化python代码存放'
    sys.path.append(path)
    import ITmonthReport


def func():
    # 清空任务
    schedule.clear()
    # 每周五8：45 执行任务
    # schedule.every().friday.at("08:45").do(every_friday_work)
    schedule.every().day.at("11:24").do(every_friday_work)
    # # 创建一个按2秒间隔执行任务
    schedule.every().day.at("11:25").do(every_friday_work)
    # schedule.every().wednesday.at("11:43").do(every_month_work)
    while True:
        schedule.run_pending()


if __name__ == '__main__':
    func()
