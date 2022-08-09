
from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets
from org.apache.nifi.processor.io import StreamCallback
from org.python.core.util import StringUtil
import json
from org.python.core import codecs
codecs.setDefaultEncoding('utf-8')


class Callback(StreamCallback):
    def __init__(self):
        self.matches = {}

    def process(self, inputStream,outputStream):
        text = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
        licitacion= json.loads(text)
        licitacion.pop("cpv",None)
        licitacionJSON= json.dumps(licitacion)
        outputStream.write(StringUtil.toBytes(licitacionJSON))

flowFile = session.get()

if flowFile != None:
    callback = Callback()
    session.write(flowFile, callback)
    session.transfer(flowFile, REL_SUCCESS)
    session.commit()
