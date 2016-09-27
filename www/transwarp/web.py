# web.py
# -*- encoding: utf-8 -*-

import threading
import functools
import types
import re

# 全局ThreadLocal对象：
ctx = threading.local()

# HTTP错误类
class HttpError(Exception):
    pass



class Request(object):
    """
    请求对象
    """

    def __init__(self, environ):
        self._environ = environ

    # 根据key返回value：
    def get(self, key, default=None):
        pass

    # 返回key-value的dict：
    def input(self):
        pass

    # 返回URL的path：
    @property
    def path_info(self):
        pass

    # 返回HTTP Headers：
    @property 
    def headers(self):
        pass

    # 根据key返回Cookie value：
    def cookie(self, name, default=None):
        pass

# response对象：
class Response(object):
    # 设置header：
    def set_header(self, key, value):
        pass

    # 设置Cookie：
    def set_cookie(self, name, value, max_age=None, expires=None, path='/'):
        pass

    # 设置status：
    @property
    def status(self):
        pass
    @status.setter
    def status(self, value):
        pass

###################################
#   URL路由， 将URL 映射到函数上
###################################

# 用于判断url是否带参的正则
_re_route = re.compile(r'(:[a-zA-Z_]\w*)')

# 定义GET：
def get(path): 
    def _decorator(func):
        func.__web_route__ = path
        func.__web_method__ = 'GET'
        return func
    return _decorator

# 定义POST：
def post(path):
    def _decorator(func):
        func.__web_route__ = path
        func.__web_method__ = 'POST'
        return func
    return _decorator

def _build_regex(path):
    """
    将路径转化成正则表达式，并取参
    """
    re_list = ['^']
    var_list = []
    is_var = False
    for v in _re_route.split(path):
        if is_var:
            var_name = v[1:]
            var_list.append(var_name)
            re_list.append(r'(?p<%s>[^\/]+)' % var_name)
        else:
            s = ''
            for ch in v:
                if '0' <= ch <= '9':
                    s += ch
                elif 'A' <= ch <= 'Z':
                    s += ch
                elif 'a' <= ch <= 'z':
                    s += ch
                else:
                    s = s + '\\' + ch
            re_list.append(s)
        is_var = not is_var
    re_list.append('$')
    return ''.join(re_list)

class Route(object):
    """
    动态路由对象
    """
    def __init__(self, func):
        self.path = func.__web_route__
        self.method = func.__web_method__
        self.is_static = _re_route.search(self.path) is None
        if not self.is_static:
            self.route = re.compile(_build)
        self.func = func

    def match(self, url):
        """
        返回URL带的参数
        """
        m = self.route.match(url)
        if m:
            return m.groups()
        return None

    def __call__(self, *args):
        return self.func(*args)

    def __str__(self):
        if self.is_static:
            return 'Route(static,%s,path=%s)' % (self.method, self.path)
        return 'Route(dynamic,%s,path=%s)' % (self.method, self.path)

    __repr__ = __str__

# 定义模板：
def view(path):
    def _decorator(func):
        @functools.wraps(func)
        def _wrapper(*args, **kw):
            r = func(*args, **kw)
            if isinstance(r, dict):
                return Template(path, **r)
            raise ValueError('Except return a dict when using @view() decorator.')
        return _wrapper
    return _decorator

# 定义拦截器：
def interceptor(pattern):
    pass

class Template(object):
    def __init__(self, template_name, **kw):
        self.template_name = template_name
        self.model = dict(**kw)

# 定义模板引擎：
def TemplateEngine(object):
    def __call__(self, path, model):
        return '<!-- override this method to render template -->'

def jinja2TemplateEngine(TemplateEngine):
    def __init__(self, templ_dir, **kw):
        from jinja2 import Environment, FileSystemLoader
        if 'autoescape' not in kw:
            kw['autoescape'] = True
        self._env = Environment(loader=FileSystemLoader(templ_dir), **kw)

    def add_filter(self, name, fn_filter):
        self._env.filters[name] = fn_filter

    def __call__(self, path, model):
        return self._env.get_template(path).render(**model).encode('utf-8')

def _load_module(module_name):
    last_dot = module_name.rfind('.')
    if last_dot == (-1):
        return __import__(module_name, globals(), locals())
    from_module = module_name[:last_dot]
    import_module = module_name[last_dot+1:]
    m = __import__(from_module, globals(), locals(), [import_module])
    return getattr(m, import_module)

#################################
#   WSGIApplication 实现WSGI接口
#   封装 wsgi Server(run方法) 和 wsgi 处理函数
#################################
class WSGIApplication(object):
    
    def __init__(self, document_root=None, **kw):
        self._running = False
        self._document_root = document_root
        self._interceptors = []
        self._template_engine = None

        self._get_static = {}
        self._post_static = {}
        self._get_dynamic = []
        self._post_dynamic = []

    def _check_not_running(self):
        if self._running:
            raise RuntimeError('Cannot modify WSGIApplication when running.')

    @property
    def template_engine(self):
        return self._template_engine
    @template_engine.setter
    def template_engine(self, engine):
        self._check_not_running()
        self._template_engine = engine

    def add_module(self, mod):
        self._check_not_running()
        m = mod if type(mod) == types.ModuleType else _load_module(mod)
        for name in dir(m):
            fn = getattr(m, name)
            if callable(fn) and hasattr(fn, '__web_route__') and hasattr(fn, '__web_method__'):
                self.add_url(fn)

    def add_url(self, func):
        """
        添加路由
        """
        self._check_not_running()
        route = Route(func)
        if route.is_static:
            if route.method == 'GET':
                self._get_static[route.path] = route
            if route.method == 'POST':
                self._post_static[route.path] = route
        else:
            if route.method == 'GET':
                self._get_dynamic.append(route)
            if route.method == 'POST':
                self._post_dynamic.append(route)

    # 添加一个Interceptor定义：
    def add_interceptor(self, func):
        pass

    # 设置TemplateEngine：
    @property 
    def template_engine(self):
        pass
    @template_engine.setter
    def template_engine(self, engine):
        pass

    # 返回WSGI处理函数：
    def get_wsgi_application(self):
        self._check_not_running()
        def wsgi(env, start_response):
            pass
        return wsgi

    # 开发模式下直接启动服务器：
    def run(self, port=9000, host='127.0.0.1'):
        from wsgiref.simple_server import make_server
        server = make_server(host, port, self.get_wsgi_application())
        server.server_forever()
    
