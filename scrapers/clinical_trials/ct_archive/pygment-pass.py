__author__ = 'faye'
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import os
from pygments import highlight
from pygments.lexers.web import HtmlLexer
from pygments.formatters.html import HtmlFormatter


for directory, subdirectories, files in os.walk("../ct_studies"):
    for file in files:
        f = open(os.path.join(directory,file), "r+")
        highlighted = highlight(f.read(), HtmlLexer(), HtmlFormatter())
        open(os.path.join(directory,file), "w").close()
        f.write(highlighted)
        f.write('<style>'+HtmlFormatter().get_style_defs('.highlight')+'</style>')
        #print highlighted
        #print HtmlFormatter().get_style_defs('.highlight')
        f.close()