# wsgiapp.py
# -*- encoding: utf-8 -*-

import logging; logging.basicConfig(level=logging.INFO)
import os

from transwarp import db
from transwarp.web import WSGIApplication, jinja2TemplateEngine

from config import configs

# 初始化数据库
db.create_engine(**configs.db)

# 创建一个WSGIApplication
wsgi = WSGIApplication(os.path.dirname(os.path.abspath(__file__)))

# 初始化jinja2模板引擎
template_engine = jinja2TemplateEngine(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates'))
wsgi.template_engine = template_engine

# 加载带有@get/@post的URL处理函数
import urls
wsgi.add_module(urls)

# 在9000端口上启动本地测试服务器
if __name__=='__main__':
    wsgi.run(9000)