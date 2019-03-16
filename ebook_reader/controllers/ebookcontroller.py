from google.cloud import translate
import logging
import ebooklib
from ebooklib import epub
from myhtmlparser import MyHTMLParser
import re


logger = logging.getLogger("EbookControl")


class EbookController:
    def __init__(self):
        self._book = None
        self._parsed_book = dict()

    async def get_parsed_book(self):
        return self._parsed_book

    async def read_book(self, book_file):
        p = re.compile(r'\.\s+')
        p2 = re.compile(r"\\'")
        self._book = epub.read_epub(book_file)
        for item in self._book.get_items():
            if item.get_type() == ebooklib.ITEM_DOCUMENT:
                name = str(item.get_name())
                self._parsed_book[name] = list()
                logger.debug('==================================')
                logger.debug('NAME : ' + name)
                logger.debug('----------------------------------')
                content = str(item.get_content())
                logger.debug(content)
                parser = MyHTMLParser()
                parser.feed(content)
                result = parser.get_result()
                for string in result:
                    string = p.sub('.\n', string)
                    string = p2.sub("'", string)
                    lines = string.split("\n")
                    for line in lines:
                        new_line = str(line.lstrip("\\n")).rstrip()
                        self._parsed_book[name].append(new_line)
                        logger.debug(new_line)
                        #translated_string = await self.translate_text(new_line, 'en')
                        #content = content.replace(new_line, translated_string)
                        #logger.debug(":" + str(translated_string) + ":")
                logger.debug('==================================')
        logger.debug("Book:")
        logger.debug(str(self._parsed_book))
        logger.debug('==================================')

    @staticmethod
    async def translate_text(text: str, language: str) -> str:
        # Instantiates a client
        translate_client = translate.Client()

        # Translates some text into another language
        translation = translate_client.translate(
            text,
            target_language=language)

        translation_text = translation['translatedText']
        print(u'Text: {}'.format(text))
        print(u'Translation: {}'.format(translation_text))

        return translation_text