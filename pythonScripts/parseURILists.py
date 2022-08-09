import re
from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets
from org.apache.nifi.processor.io import StreamCallback
from org.python.core.util import StringUtil
import xml.etree.ElementTree as etree
import json
import urllib2
from org.python.core import codecs
codecs.setDefaultEncoding('utf-8')


def requestGet(url):

  request = urllib2.Request(url)
  try:
      data = urllib2.urlopen(request)
      return data.read()
  except:
      return None

# define a callback to use


def getListsFromURL(url):
    try:
      responseList = requestGet(url)
      xmlList = etree.fromstring(responseList)
      xmlRows = xmlList.findall(".//SimpleCodeList/Row")
      newList = []
      for row in xmlRows:
          code = row.findall("Value[@ColumnRef='code']/SimpleValue")[0].text
          nombre = row.findall(
              "Value[@ColumnRef='nombre']/SimpleValue")[0].text
          newList.append(
            {
                "url":url,
                "cod":code,
                "valor":nombre
            }
          ) 
      return newList
    except:
        return [
            {
                "url":"",
                "cod":"",
                "valor":""
            }
        ]

class Callback(StreamCallback):
    def __init__(self):
        pass

    def process(self, inputStream, outputStream):
        text = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
        objectURIS=getListsFromURL(text)
        output_text = json.dumps(list(objectURIS))
        outputStream.write(StringUtil.toBytes(output_text))

flowFile= session.get()

if flowFile!= None:
    flowFile=session.write(flowFile,Callback())

    session.transfer(flowFile,REL_SUCCESS)

    
    
