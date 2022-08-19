# import a few libraries to use for reading in the flow files
from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets
from org.apache.nifi.processor.io import StreamCallback
from org.python.core.util import StringUtil
import xml.etree.ElementTree as etree
import json
import re
from org.python.core import codecs
codecs.setDefaultEncoding('utf-8')

def getFileName(url):
    return re.sub(r'(.+)\/', "", url)


def convertXMLToJSON(content):
    entry = etree.fromstring(content)

    namespaces = {
        "atom": "http://www.w3.org/2005/Atom",
        "cbc-place-ext": "urn:dgpe:names:draft:codice-place-ext:schema:xsd:CommonBasicComponents-2",
        "cac-place-ext": "urn:dgpe:names:draft:codice-place-ext:schema:xsd:CommonAggregateComponents-2",
        "cbc": "urn:dgpe:names:draft:codice:schema:xsd:CommonBasicComponents-2",
        "cac": "urn:dgpe:names:draft:codice:schema:xsd:CommonAggregateComponents-2",
        "ns1": "urn:oasis:names:specification:ubl:schema:xsd:CommonExtensionComponents-2",
        "at": "http://purl.org/atompub/tombstones/1.0"
    }
    # listTags=root.findall(".//*[@listURI]",namespaces=namespaces)

    #listURIS=set([tags.attrib["listURI"] for tags in listTags]);

    #objectURLS= getListsFromURL(listURIS)

    # entries=root.findall("atom:entry",namespaces=namespaces);

    # licitaciones=[]

    id_lic = entry.findall("atom:id", namespaces=namespaces)[
        0].text.split('/')[-1]

    link = entry.findall("atom:link", namespaces=namespaces)[0].attrib['href']

    fecha_actualizacion = entry.findall(
        "atom:updated", namespaces=namespaces)[0].text

    _ContractFolderStatus = entry.findall(
        "cac-place-ext:ContractFolderStatus", namespaces=namespaces)[0]

    numero_expediente = _ContractFolderStatus.findall(
        "cbc:ContractFolderID", namespaces=namespaces)[0].text

    tag_estado = _ContractFolderStatus.findall(
        "cbc-place-ext:ContractFolderStatusCode", namespaces=namespaces)[0]
    uri_estado = tag_estado.attrib['listURI']
    cod_estado = tag_estado.text
    estado = {
        "cod": cod_estado,
        "url": getFileName(uri_estado)
    }

    _LocatedContractingParty = _ContractFolderStatus.findall(
        "cac-place-ext:LocatedContractingParty", namespaces=namespaces)[0]

    tag_tipo_administracion = _LocatedContractingParty.findall(
        "cbc:ContractingPartyTypeCode", namespaces=namespaces)[0]
    cod_tipo_administracion = tag_tipo_administracion.text
    uri_tipo_administracion = tag_tipo_administracion.attrib['listURI']
    tipo_administracion = {
        "cod": cod_tipo_administracion,
        "url": getFileName(uri_tipo_administracion)
    }

    _Party = _LocatedContractingParty.findall(
        "cac:Party", namespaces=namespaces)[0]

    dir3 = ""
    try:
        dir3 = _Party.findall(
            ".//*[@schemeName='DIR3']", namespaces=namespaces)[0].text
    except:
        pass

    nif_oc = _Party.findall(
        ".//*[@schemeName='NIF']", namespaces=namespaces)[0].text

    organo_contratacion = _Party.findall(
        "cac:PartyName/cbc:Name", namespaces=namespaces)[0].text

    codigo_postal = _Party.findall(
        "cac:PostalAddress/cbc:PostalZone", namespaces=namespaces)[0].text

    _ProcurementProject = _ContractFolderStatus.findall(
        "cac:ProcurementProject", namespaces=namespaces)[0]

    objeto = _ProcurementProject.findall(
        "cbc:Name", namespaces=namespaces)[0].text

    tag_tipo_contrato = _ProcurementProject.findall(
        "cbc:TypeCode", namespaces=namespaces)[0]
    cod_tipo_contrato = tag_tipo_contrato.text
    uri_tipo_contrato = tag_tipo_contrato.attrib["listURI"]
    tipo_contrato = {"cod": cod_tipo_contrato, "url": getFileName(uri_tipo_contrato)}

    _BudgetAmount = _ProcurementProject.findall(
        "cac:BudgetAmount", namespaces=namespaces)[0]

    try:
        valor_estimado_contrato = _BudgetAmount.findall(
            "cbc:EstimatedOverallContractAmount", namespaces=namespaces)[0].text
    except:
        valor_estimado_contrato = None

    presupuesto_con_impuestos = _BudgetAmount.findall(
        "cbc:TaxExclusiveAmount", namespaces=namespaces)[0].text

    presupuesto_base = _BudgetAmount.findall(
        "cbc:TotalAmount", namespaces=namespaces)[0].text

    CPVTags = _ProcurementProject.findall(
        "cac:RequiredCommodityClassification/cbc:ItemClassificationCode", namespaces=namespaces)
    cpv_lista = [{"id_lic": id_lic, "fecha_actualizacion": fecha_actualizacion, "url": getFileName(CPVTag.attrib['listURI']), "cod":CPVTag.text}
                 for CPVTag in CPVTags]

    if len(cpv_lista) == 0:
        cpv_lista = [{
            "id_lic": id_lic,
            "fecha_actualizacion": fecha_actualizacion,
            "cod": "",
            "url": ""
        }]

    lugar_ejecucion = {
        "cod": "",
        "url": ""
    }
    try:
        tag_lugar_ejecucion = _ProcurementProject.findall(
            "cac:RealizedLocation/cbc:CountrySubentityCode", namespaces=namespaces)[0]
        cod_lugar_ejecucion = tag_lugar_ejecucion.text
        uri_lugar_ejecucion = tag_lugar_ejecucion.attrib['listURI']
        lugar_ejecucion = {
            "cod": cod_lugar_ejecucion,
            "url": getFileName(uri_lugar_ejecucion)
        }
    except:
        pass

    _TenderingProcess = {}
    tipo_procedimiento = {
        "cod": "",
        "url": ""
    }
    try:
        _TenderingProcess = _ContractFolderStatus.findall(
            "cac:TenderingProcess", namespaces=namespaces)[0]

        tag_tipo_procedimiento = _TenderingProcess.findall(
            "cbc:ProcedureCode", namespaces=namespaces)[0]
        cod_tipo_procedimiento = tag_tipo_procedimiento.text
        uri_tipo_procedimiento = tag_tipo_procedimiento.attrib["listURI"]
        tipo_procedimiento = {
            "cod": cod_tipo_procedimiento,
            "url": getFileName(uri_tipo_procedimiento)
        }
    except:
        pass

    tramitacion = {
        "cod": "",
        "url": ""
    }
    try:
        tag_tramitacion = _TenderingProcess.findall(
            "cbc:UrgencyCode", namespaces=namespaces)[0]
        cod_tramitacion = tag_tramitacion.text
        uri_tramitacion = tag_tramitacion.attrib["listURI"]
        tramitacion = {
            "cod": cod_tramitacion,
            "url": getFileName(uri_tramitacion)
        }
    except:
        pass

    sistema_contratacion = {
        "cod": "",
        "url": ""
    }
    try:
        tag_sistema_contratacion = _TenderingProcess.findall(
            "cbc:ContractingSystemCode", namespaces=namespaces)[0]
        cod_sistema_contratacion = tag_sistema_contratacion.text
        uri_sistema_contratacion = tag_sistema_contratacion.attrib["listURI"]

        sistema_contratacion = {
            "cod": cod_sistema_contratacion,
            "url": getFileName(uri_sistema_contratacion)
        }
    except:
        pass

    forma_presentacion_oferta = {
        "cod": "",
        "url": ""
    }
    try:
        tag_presentacion_oferta = _TenderingProcess.findall(
            "cbc:SubmissionMethodCode", namespaces=namespaces)[0]
        cod_presentacion_oferta = tag_presentacion_oferta.text
        uri_presentacion_oferta = tag_presentacion_oferta.attrib["listURI"]
        forma_presentacion_oferta = {
            "cod": cod_presentacion_oferta,
            "url": getFileName(uri_presentacion_oferta)
        }
    except:
        pass
    fecha_presentacion_oferta = ""
    try:
        fecha_corta_presentacion_oferta = _TenderingProcess.findall(
            "cac:TenderSubmissionDeadlinePeriod/cbc:EndDate", namespaces=namespaces)[0].text
        hora_presentacion_oferta = _TenderingProcess.findall(
            "cac:TenderSubmissionDeadlinePeriod/cbc:EndTime", namespaces=namespaces)[0].text

        fecha_presentacion_oferta = "%s %s" % (
            fecha_corta_presentacion_oferta, hora_presentacion_oferta)

        # fecha_presentacion_oferta= f"{fecha_corta_presentacion_oferta} {hora_presentacion_oferta}"
    except:
        pass

    directiva_aplicacion = ""
    subcontratacion = ""
    porcentaje_subcontratacion = "0"
    try:
        _TenderingTerms = _ContractFolderStatus.findall(
            "cac:TenderingTerms")[0]
        directiva_aplicacion = _TenderingTerms.findall(
            "cac:ProcurementLegislationDocumentReference/cbc:ID", namespaces=namespaces)[0].text
        subcontratacion = _TenderingTerms.findall(
            "cac:AllowedSubcontractTerms/cbc:Description", namespaces=namespaces)[0].text
        porcentaje_subcontratacion = _TenderingTerms.findall(
            "cac:AllowedSubcontractTerms/cbc:Rate", namespaces=namespaces)[0].text
    except:
        pass

    funding_code = {
        "cod": "",
        "url": ""
    }
    funding_program = ""
    try:
        tag_funding_code = _ContractFolderStatus.findall(
            "cac:TenderingTerms/cbc:FundingProgramCode")[0]
        cod_funding_code = tag_funding_code.text
        uri_funding_code = tag_funding_code.attrib["listURI"]
        funding_code = {
            "cod": cod_funding_code,
            "url": getFileName(uri_funding_code)
        }
        funding_program = _ContractFolderStatus.findall(
            "cac:TenderingTerms/cbc:FundingProgram")[0].text
    except:
        pass

    _ProcurementProjectLotList = _ContractFolderStatus.findall(
        "cac:ProcurementProjectLot", namespaces=namespaces)

    cantidad_lotes = len(_ProcurementProjectLotList)

    lotes = {
        "0": {
            "id_lic": id_lic,
            "fecha_actualizacion": fecha_actualizacion,
            "lote": "",
            "objeto": objeto,
            "presupuesto_base": "0",
            "presupuesto_impuestos": "0",
            "cpv": [
                {
                    "id_lic": id_lic,
                    "fecha_actualizacion": fecha_actualizacion,
                    "id_lote": "",
                    "cod": "",
                    "url": ""
                }
            ],
        }
    }

    for tag_lote in _ProcurementProjectLotList:
        id_lote = ""
        id_lote = tag_lote.findall(
            "*[@schemeName='ID_LOTE']", namespaces=namespaces)[0].text

        objeto_lote = objeto
        importe_sin_impuestos = "0"
        importe_con_impuestos = "0"
        cpv_lotes = []
        try:
            _loteProcurementProject = tag_lote.findall(
                "cac:ProcurementProject", namespaces=namespaces)[0]

            objeto_lote = _loteProcurementProject.findall(
                "cbc:Name", namespaces=namespaces)[0].text

            objeto_lote = 'Lote %s: %s' % (id_lote, objeto_lote)
            importe_sin_impuestos = _loteProcurementProject.findall(
                "cac:BudgetAmount/cbc:TotalAmount", namespaces=namespaces)[0].text
            importe_con_impuestos = _loteProcurementProject.findall(
                "cac:BudgetAmount/cbc:TaxExclusiveAmount", namespaces=namespaces)[0].text
            TagsCPVLotes = _loteProcurementProject.findall(
                "cac:RequiredCommodityClassification/cbc:ItemClassificationCode", namespaces=namespaces)

            cpv_lotes = [{"id_lic": id_lic, "id_lote": id_lote, "fecha_actualizacion":fecha_actualizacion, "url": getFileName(CPVTag.attrib['listURI']),
                          "cod":CPVTag.text} for CPVTag in TagsCPVLotes]

            if len(cpv_lotes) == 0:
                cpv_lotes = [
                    {
                        "id_lic": id_lic,
                        "id_lote": id_lote,
                        "cod": "",
                        "url": ""
                    }
                ]

        except:
            pass
        if cantidad_lotes > 0:
            lotes[str(id_lote)] = {
                "id_lic": id_lic,
                "fecha_actualizacion": fecha_actualizacion,
                "lote": id_lote,
                "objeto": objeto_lote,
                "presupuesto_base": importe_sin_impuestos,
                "presupuesto_impuestos": importe_con_impuestos,
                "cpv": cpv_lotes,
            }
        else:
            lotes["0"] = {
                "id_lic": id_lic,
                "fecha_actualizacion": fecha_actualizacion,
                "lote": "",
                "objeto": objeto_lote,
                "presupuesto_base": importe_sin_impuestos,
                "presupuesto_impuestos": importe_con_impuestos,
                "cpv": cpv_lotes,
            }

    tags_resultados_lotes = _ContractFolderStatus.findall(
        "cac:TenderResult", namespaces=namespaces)
    cantidad_resultados = len(tags_resultados_lotes)
    if cantidad_resultados == 0:
        lotes["0"]["cod_resultado"] = ""
        lotes["0"]["url_resultado"] = ""
        lotes["0"]["tipo_identificador"] = ""
        lotes["0"]["adjudicatario"] = ""
        lotes["0"]["identificador_adjudicatario"] = ""
        lotes["0"]["esPyme"] = "NO"
        lotes["0"]["importe_adjudicacion_base"] = "0"
        lotes["0"]["importe_adjudicacion_impuestos"] = "0"
        lotes["0"]["numero_contrato"] = ""
        lotes["0"]["fecha_formalizacion"] = ""
        lotes["0"]["fecha_entrada_vigor"] = ""
        lotes["0"]["fecha_acuerdo"] = ""
        lotes["0"]["numero_ofertas"] = "0"
        lotes["0"]["oferta_mas_baja"] = "0"
        lotes["0"]["oferta_mas_alta"] = "0"
        lotes["0"]["ofertas_anormales"] = ""

    for tag_resultado_lote in tags_resultados_lotes:
        id_resultado_lote = "0"
        resultado = {
            "cod": "",
            "url": ""
        }
        adjudicatario = ""
        tipo_identificador = ""
        identificador_adjudicatario = ""
        esPyme = "NO"
        importe_adjudicacion_base = "0"
        importe_adjudicacion_impuestos = "0"

        try:
            id_resultado_lote = tag_resultado_lote.findall(
                "cac:AwardedTenderedProject/cbc:ProcurementProjectLotID", namespaces=namespaces)[0].text
        except:
            pass
        tag_resultado = tag_resultado_lote.findall(
            "cbc:ResultCode", namespaces=namespaces)[0]

        cod_resultado = tag_resultado.text

        uri_resultado = tag_resultado.attrib["listURI"]

        resultado = {
            "cod": cod_resultado,
            "url": getFileName(uri_resultado)
        }

        try:
            _WinningParty = tag_resultado_lote.findall(
                "cac:WinningParty", namespaces=namespaces)[0]

            adjudicatario = _WinningParty.findall(
                "cac:PartyName/cbc:Name", namespaces=namespaces)[0].text

            tag_id_adjud = _WinningParty.findall(
                "cac:PartyIdentification/cbc:ID", namespaces=namespaces)[0]

            tipo_identificador = tag_id_adjud.attrib["schemeName"]

            identificador_adjudicatario = tag_id_adjud.text

            esPyme = tag_resultado_lote.findall(
                "cbc:SMEAwardedIndicator", namespaces=namespaces)[0].text

            importe_adjudicacion_base = tag_resultado_lote.findall(
                "cac:AwardedTenderedProject/cac:LegalMonetaryTotal/cbc:TaxExclusiveAmount", namespaces=namespaces)[0].text

            importe_adjudicacion_impuestos = tag_resultado_lote.findall(
                "cac:AwardedTenderedProject/cac:LegalMonetaryTotal/cbc:PayableAmount", namespaces=namespaces)[0].text
        except:
            pass

        fecha_acuerdo = ""
        numero_ofertas = "0"
        oferta_mas_baja = "0"
        oferta_mas_alta = "0"
        ofertas_anormales = ""
        try:
            fecha_acuerdo = tag_resultado_lote.findall(
                "cbc:AwardDate", namespaces=namespaces)[0].text
            numero_ofertas = tag_resultado_lote.findall(
                "cbc:ReceivedTenderQuantity", namespaces=namespaces)[0].text
            oferta_mas_baja = tag_resultado_lote.findall(
                "cbc:LowerTenderAmount", namespaces=namespaces)[0].text
            oferta_mas_alta = tag_resultado_lote.findall(
                "cbc:HigherTenderAmount", namespaces=namespaces)[0].text
            ofertas_anormales = tag_resultado_lote.findall(
                "cbc:AbnormallyLowTendersIndicator", namespaces=namespaces)[0].text
        except:
            pass
        numero_contrato = ""
        fecha_formalizacion = ""
        fecha_entrada_vigor = ""
        try:
            numero_contrato = tag_resultado_lote.findall(
                "cac:Contract/cbc:ID", namespaces=namespaces)[0].text
            fecha_formalizacion = tag_resultado_lote.findall(
                "cac:Contract/cbc:IssueDate", namespaces=namespaces)[0].text
            fecha_entrada_vigor = tag_resultado_lote.findall("cbc:StartDate")[
                0].text
        except:
            pass

        lotes[str(id_resultado_lote)]["cod_resultado"] = resultado["cod"]
        lotes[str(id_resultado_lote)]["url_resultado"] = resultado["url"]
        lotes[str(id_resultado_lote)]["tipo_identificador"] = tipo_identificador
        lotes[str(id_resultado_lote)]["adjudicatario"] = adjudicatario
        lotes[str(id_resultado_lote)
              ]["identificador_adjudicatario"] = identificador_adjudicatario
        lotes[str(id_resultado_lote)]["esPyme"] = esPyme
        lotes[str(id_resultado_lote)
              ]["importe_adjudicacion_base"] = importe_adjudicacion_base
        lotes[str(id_resultado_lote)
              ]["importe_adjudicacion_impuestos"] = importe_adjudicacion_impuestos
        lotes[str(id_resultado_lote)]["numero_contrato"] = numero_contrato
        lotes[str(id_resultado_lote)
              ]["fecha_formalizacion"] = fecha_formalizacion
        lotes[str(id_resultado_lote)
              ]["fecha_entrada_vigor"] = fecha_entrada_vigor
        lotes[str(id_resultado_lote)]["fecha_acuerdo"] = fecha_acuerdo
        lotes[str(id_resultado_lote)]["numero_ofertas"] = numero_ofertas
        lotes[str(id_resultado_lote)]["oferta_mas_baja"] = oferta_mas_baja
        lotes[str(id_resultado_lote)]["oferta_mas_alta"] = oferta_mas_alta
        lotes[str(id_resultado_lote)]["ofertas_anormales"] = ofertas_anormales

    lista_lotes = []
    try:
        lista_lotes = list(lotes.values())
    except:
        pass

    licitacion = {
        "id": id_lic,
        "link": link,
        "situacion": "VIGENTE",
        "fecha_actualizacion": fecha_actualizacion,
        "numero_expediente": numero_expediente,
        "estado": estado,
        "tipo_administracion": tipo_administracion,
        "dir3": dir3,
        "nif_oc": nif_oc,
        "organo_contratacion": organo_contratacion,
        "codigo_postal": codigo_postal,
        "objeto": objeto,
        "tipo_contrato": tipo_contrato,
        "valor_estimado_contrato": valor_estimado_contrato,
        "presupuesto_base": presupuesto_base,
        "presupuesto_con_impuestos": presupuesto_con_impuestos,
        "cpv": cpv_lista,
        "lugar_ejecucion": lugar_ejecucion,
        "tipo_procedimiento": tipo_procedimiento,
        "tramitacion": tramitacion,
        "sistema_contratacion": sistema_contratacion,
        "forma_presentacion_oferta": forma_presentacion_oferta,
        "fecha_presentacion_oferta": fecha_presentacion_oferta,
        "directiva_aplicacion": directiva_aplicacion,
        "subcontratacion": subcontratacion,
        "porcentaje_subcontratacion": porcentaje_subcontratacion,
        "funding_code": funding_code,
        "funding_program": funding_program,
        "resultados": lista_lotes
    }
    return licitacion


class Callback(StreamCallback):
    def __init__(self):
        self.matches = {}

    def process(self, inputStream, outputStream):
        text = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
        output_text = json.dumps(convertXMLToJSON(text))
        outputStream.write(StringUtil.toBytes(output_text))


flowFile = session.get()

if flowFile != None:
    filename = flowFile.getAttribute("filename")
    callback = Callback()
    session.write(flowFile, callback)
    filename = filename.split(".")[0] + ".json"
    flowFile = session.putAttribute(flowFile, "filename", filename)
    session.transfer(flowFile, REL_SUCCESS)
    session.commit()
