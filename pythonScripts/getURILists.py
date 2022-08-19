from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets
from org.apache.nifi.processor.io import OutputStreamCallback
from org.python.core.util import StringUtil
import xml.etree.ElementTree as etree
from org.python.core import codecs
codecs.setDefaultEncoding('utf-8')

# define a callback to use
def convertXMLToURIS(content):
  root= etree.fromstring(content)
  namespaces={
      "atom":"http://www.w3.org/2005/Atom",
      "cbc-place-ext":"urn:dgpe:names:draft:codice-place-ext:schema:xsd:CommonBasicComponents-2",
      "cac-place-ext":"urn:dgpe:names:draft:codice-place-ext:schema:xsd:CommonAggregateComponents-2",
      "cbc":"urn:dgpe:names:draft:codice:schema:xsd:CommonBasicComponents-2" ,
      "cac":"urn:dgpe:names:draft:codice:schema:xsd:CommonAggregateComponents-2" ,
      "ns1":"urn:oasis:names:specification:ubl:schema:xsd:CommonExtensionComponents-2",
      "at":"http://purl.org/atompub/tombstones/1.0"
  }
  listTags=root.findall(".//*[@listURI]",namespaces=namespaces)

  listURIS=set([tags.attrib["listURI"] for tags in listTags]);

  return listURIS


""" class PyInputStreamCallback(InputStreamCallback):
  def __init__(self):
     self.uri=None
  def process(self, inputStream):
    text = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
    self.uri=text """


class Callback(OutputStreamCallback):
    def __init__(self,url):
        self.url=url
        pass

    def process(self, outputStream):
        outputStream.write(StringUtil.toBytes(self.url))

flowFile= session.get()

if flowFile!= None:
    inputStream= session.read(flowFile)
    text = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
    inputStream.close()
    session.remove(flowFile)
    objectURIS=convertXMLToURIS(text)
    newflowFiles=[]
    signal_event= flowFile.getAttribute("signal_event")
    for url in objectURIS:
      newFlowFile= session.create()
      callback = Callback(url)
      newFlowFile=session.write(newFlowFile,callback)
      
      url= url.replace("/","")
      url= url.replace(":","")
      newFlowFile=session.putAttribute(newFlowFile,"filename",url+".text")
      newFlowFile= session.putAttribute(newFlowFile,"signal_event",signal_event)
      newflowFiles.append(newFlowFile)
    for newFlowFile in newflowFiles:
      session.transfer(newFlowFile,REL_SUCCESS)

    
    
