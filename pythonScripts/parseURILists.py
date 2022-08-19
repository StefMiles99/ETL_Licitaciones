import re
from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets
from org.apache.nifi.processor.io import OutputStreamCallback
from org.python.core.util import StringUtil
import xml.etree.ElementTree as etree
import json
from org.python.core import codecs
codecs.setDefaultEncoding('utf-8')



# define a callback to use


def getListsFromURL(responseList,filename):
    try:
      xmlList = etree.fromstring(responseList)
      xmlRows = xmlList.findall(".//SimpleCodeList/Row")
      newList = []
      for row in xmlRows:
          code = row.findall("Value[@ColumnRef='code']/SimpleValue")[0].text
          nombre = row.findall(
              "Value[@ColumnRef='nombre']/SimpleValue")[0].text
          newList.append(
            {
                "url":filename,
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
class Callback(OutputStreamCallback):
    def __init__(self,output_text):
        self.output_text=output_text
        pass

    def process(self, outputStream):
        outputStream.write(StringUtil.toBytes(self.output_text))

flowFile= session.get()

if flowFile!= None:
    inputStream= session.read(flowFile);
    
    text = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
    
    inputStream.close();

    filename= flowFile.getAttribute("filename").replace(".xml","").replace(".txt","");

    objectURIS=getListsFromURL(text,filename)
    
    output_text = json.dumps(list(objectURIS))
    
    flowFile=session.write(flowFile,Callback(output_text))

    session.transfer(flowFile,REL_SUCCESS)



    
    
