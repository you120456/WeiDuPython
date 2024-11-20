import httpx
from lxml import etree
import re

class LiePin:

    def __init__(self):
        heads = {
            "Cookie": "__gc_id=2000adad7b6c4e81b1a01b6f989bdbaf; __uuid=1653029416148.88; __s_bid=e4820ffa64ad64572696a8bcc02b48356bcc; Hm_lvt_a2647413544f5a04f00da7eee0d5e200=1653029417,1653637931,1654411017; __tlog=1654411016751.73|00000000|00000000|s_o_009|s_o_009; _fecdn_=1; m_city_site=; index-app-banner=yes; acw_tc=2760829416544136854697594e695a0f45c4eace7d89b8ffc6c96be92c95f2; fe_se=-1654413822955; user_roles=0; need_bind_tel=false; new_user=true; c_flag=391b72bbb413fc1fe87566aaa74f0231; user_photo=5f8fa3a679c7cc70efbf444e08u.png; user_name=%E8%93%9D%E6%A1%A5; inited_user=4709b3b1dd3741cee698656df6a6857f; JSESSIONID=B3BCC376B1D780365AB0932568BFD58C; UniqueKey=ca080e11cf0dabffa9dba665e9526a14; lt_auth=77wDOXUGzA397SKKjTZf4v1OjdutBW%2FM8CsMh0wIgda4XPSz4P%2FgQA%2BAr7YP%2FioIqxpydPUzMLb7Mez%2FzXFJ7kAT%2BVGklZyuv%2F%2B6z3sEUe1sHuyflMXuqsjQQ50mrXg6ykpgn2si; __session_seq=56; __uv_seq=56; Hm_lpvt_a2647413544f5a04f00da7eee0d5e200=1654414235",
            "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 13_2_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.3 Mobile/15E148 Safari/604.1",
            "Host": "m.liepin.com"
        }
        self.heads = heads

    def getLink(self,keyworld,page):
        """
        方法用于获取招聘链接
        """
        for i in range(page):
            url = "https://m.liepin.com/zhaopin/pn{}/?keyword={}&d_curPage=0&d_pageSize=20".format(i,keyworld)
            try:
                # 建立请求
                req = httpx.get(url=url,headers=self.heads)
            except :
                print("招聘列表网络访问异常!")
                return
            if req.status_code == 200:
                # 获取源码
                html = req.text
                # 获取所有招聘链接
                html = etree.HTML(html)
                job_href = html.xpath("//div[@class='so-job-job-list']/a[@class='job-card']/@href")
                return job_href
            else:
                print("招聘列表访问状态异常!")

    def getInfo(self,jobList):
        """
            获取招聘信息
        """
        job_list = []
        for item in jobList:
            try:
                req = httpx.get(url=item,headers=self.heads)
            except:
                print("招聘详情网络访问异常!")
                return
            if req.status_code == 200:
                try:
                    # 解析数据
                    html = etree.HTML(req.text)
                    # 岗位名称
                    job_name = html.xpath("//h1/span/text()")
                    # 标签
                    job_tag = html.xpath("//p[@class='clearfix']/span/text()")
                    # 招聘内容
                    job_content =  html.xpath("//div[@class='job-describe-duty']/p/text()")
                    # 公司名称
                    company_name = html.xpath("//div[@class='profile-card-title ellipsis-1']/text()")[0]
                    company_name = re.sub(r"\s","",company_name)
                    # 公司领域
                    company_card = html.xpath("//div[@class='profile-card-desc ellipsis-1']/span[1]/text()")[0]
                    # 公司规模
                    company_scale = html.xpath("//div[@class='profile-card-desc ellipsis-1']/span[2]/text()")[0]
                    # 公司类型
                    company_type = html.xpath("//div[@class='profile-card-desc ellipsis-1']/span[3]/text()")[0]
                    # 公司链接
                    company_url = html.xpath("//dl[@class='job-company-info-base']/a[1]/@href")[0]
                    if company_url == "javascript:;":
                        company_url = "无"

                    # 封装成字典
                    dic_content = {"content":
                                       {"job":{"job_name":job_name,"job_tag":job_tag,"job_content":job_content,"item":item},
                                        "company":{"company_name":company_name,"company_card":company_card,"company_scale":company_scale,"company_type":company_type,"company_url":company_url}
                                        }
                                   }
                    job_list.append(dic_content)
                except:
                    continue
            else:
                print("招聘列表访问状态异常!")

        return job_list


if __name__ == '__main__':
    lp = LiePin()
    links = lp.getLink("Python",2)
    data = lp.getInfo(links)
    print(data)
