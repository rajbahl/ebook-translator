from html.parser import HTMLParser
import re


class MyHTMLParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self._result = list()

    def handle_data(self, data):
        test = data.lstrip('\\n').rstrip()
        if re.search('[a-zA-Z]', test):
            self._result.append(data)

    def get_result(self):
        return self._result
