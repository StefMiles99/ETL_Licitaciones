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
        licitacion["sistema_contratacion_cod"]=licitacion["sistema_contratacion"]["cod"]
        licitacion["sistema_contratacion_url"]=licitacion["sistema_contratacion"]["url"]
        licitacion.pop("sistema_contratacion",None)

        
        licitacion["funding_code_cod"]=licitacion["funding_code"]["cod"]
        licitacion["funding_code_url"]=licitacion["funding_code"]["url"]
        licitacion.pop("funding_code",None)

        licitacion["estado_cod"]=licitacion["estado"]["cod"]
        licitacion["estado_url"]=licitacion["estado"]["url"]
        licitacion.pop("estado",None)

        licitacion["lugar_ejecucion_cod"]=licitacion["lugar_ejecucion"]["cod"]
        licitacion["lugar_ejecucion_url"]=licitacion["lugar_ejecucion"]["url"]
        licitacion.pop("lugar_ejecucion",None)

        licitacion["tipo_procedimiento_cod"]=licitacion["tipo_procedimiento"]["cod"]
        licitacion["tipo_procedimiento_url"]=licitacion["tipo_procedimiento"]["url"]
        licitacion.pop("tipo_procedimiento",None)

        licitacion["tipo_administracion_cod"]=licitacion["tipo_administracion"]["cod"]
        licitacion["tipo_administracion_url"]=licitacion["tipo_administracion"]["url"]
        licitacion.pop("tipo_administracion",None)

        licitacion["forma_presentacion_oferta_cod"]=licitacion["forma_presentacion_oferta"]["cod"]
        licitacion["forma_presentacion_oferta_url"]=licitacion["forma_presentacion_oferta"]["url"]
        licitacion.pop("forma_presentacion_oferta",None)

        licitacion["tramitacion_cod"]=licitacion["tramitacion"]["cod"]
        licitacion["tramitacion_url"]=licitacion["tramitacion"]["url"]
        licitacion.pop("tramitacion",None)

        licitacion["tipo_contrato_cod"]=licitacion["tipo_contrato"]["cod"]
        licitacion["tipo_contrato_url"]=licitacion["tipo_contrato"]["url"]
        licitacion.pop("tipo_contrato",None)

        licitacion.pop("resultados",None)
        licitacion.pop("cpv",None)
        licitacionJSON= json.dumps(licitacion)
        outputStream.write(StringUtil.toBytes(licitacionJSON))

flowFile = session.get()

if flowFile != None:
    callback = Callback()
    session.write(flowFile, callback)
    session.transfer(flowFile, REL_SUCCESS)
    session.commit()
