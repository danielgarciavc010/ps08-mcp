"""
MCP Server - kyoe-consultas
Transporte: SSE (HTTP)

Arranque:
    python server.py
    → escucha en http://0.0.0.0:8000/sse

Herramientas:
    - consultar_comisarias
    - consultar_cita_dnie
    - alta_cita_dnie
    - anular_cita_dnie
    - modificar_cita_dnie
    - enviar_sms
    - crear_codigo_peticion
    - buscar_codigo_localidad   (NUEVA: nombre -> codigos INE)
    - consultar_slots_comisaria
"""

import os
import csv
import httpx
import string
import secrets
import unicodedata
from datetime import datetime
from fastmcp import FastMCP
from rapidfuzz import fuzz

# ──────────────────────────────────────────────
# Configuración
# ──────────────────────────────────────────────
BASE_URL = "http://10.118.185.114:8080"
TIMEOUT  = 15.0
HOST     = "0.0.0.0"
PORT     = 8000

# Maximo de comisarias que se devuelven al agente. El servicio puede devolver
# decenas (p. ej. 35 en Madrid); enviarlas todas satura al modelo. Recortamos
# a unas pocas, ya limpias y numeradas, para que el agente no se atasque.
MAX_COMISARIAS = 5

# Umbral (0-100) para el emparejamiento difuso de comisaria por nombre/direccion.
# Solo se usa como respaldo cuando la coincidencia por subcadena no encuentra
# nada, para tolerar erratas sin colar coincidencias flojas.
UMBRAL_FUZZY = 85

# CSV de codigos INE, ubicado junto a este server.py (ruta relativa)
CSV_CODIGOS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "codigos_ine.csv")

mcp = FastMCP(
    name="kyoe-consultas",
    instructions=(
        "Herramientas para consultar comisarías disponibles, consultar, dar de "
        "alta, anular y modificar citas de DNI/NIE/pasaporte a través de los "
        "servicios de rag.kyoe.es, para enviar SMS y consultar slots disponibles."
    ),
)

# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────
def _error(code: str, message: str) -> dict:
    return {"ok": False, "error": {"code": code, "message": message}}


def _validar_requeridos(**campos):
    """Devuelve un _error si algun campo requerido viene vacio; None si estan
    todos. Evita que el agente llame a la API con parametros en blanco."""
    for nombre, valor in campos.items():
        if not str(valor or "").strip():
            return _error(
                "FALTA_DATO",
                f"Falta el dato '{nombre}'. Preguntaselo al ciudadano antes de continuar.",
            )
    return None


def _inferir_tipo_documento(numero_documento: str):
    """Deduce el tipo de documento a partir del numero, sin pedirselo al agente:
    - NIE: empieza por X, Y o Z -> 'X'
    - DNI: empieza por digito   -> 'D'
    Devuelve (tipo, error): si no encaja en ninguno, tipo es None y error es un
    _error(...)."""
    doc = str(numero_documento or "").strip().upper()
    if not doc:
        return None, _error(
            "FALTA_DATO",
            "Falta el numero de documento. Preguntaselo al ciudadano antes de continuar.",
        )
    if doc[0] in {"X", "Y", "Z"}:
        return "X", None
    if doc[0].isdigit():
        return "D", None
    return None, _error(
        "INVALID_PARAM",
        "El numero de documento no tiene un formato valido de DNI o NIE.",
    )


# Letras de control para DNI/NIE (algoritmo modulo 23 de la DGP).
_LETRAS_CONTROL = "TRWAGMYFPDXBNJZSQVHLCKE"


def _es_documento_ejemplo(digitos: str) -> bool:
    """True si el bloque de digitos parece un numero de ejemplo/inventado: todos
    los digitos iguales (00000000, 11111111...) o una secuencia estrictamente
    ascendente/descendente (12345678, 87654321...). El modelo cuantizado fabrica
    justo estos cuando el ciudadano da muchos datos y no incluye el documento
    real (tipico: el 12345678Z de los ejemplos). Aunque el checksum sea valido,
    no son documentos reales, asi que los rechazamos para forzar que se pidan."""
    d = str(digitos or "")
    if not d.isdigit() or len(d) < 2:
        return False
    if len(set(d)) == 1:
        return True
    ascendente = all(int(d[i + 1]) - int(d[i]) == 1 for i in range(len(d) - 1))
    descendente = all(int(d[i]) - int(d[i + 1]) == 1 for i in range(len(d) - 1))
    return ascendente or descendente


def _normalizar_id_tramite(id_tramite: str) -> str:
    """Traduce el tramite que indica el agente ('DNI', 'NIE' o 'PASAPORTE', en
    mayusculas o minusculas) al codigo que espera la API: DNI y NIE -> 'DNIE';
    PASAPORTE -> 'PASAPORTE'. Si viene vacio, devuelve '' para que la API use su
    tramite por defecto."""
    t = str(id_tramite or "").strip().upper()
    if t in {"DNI", "NIE", "DNIE"}:
        return "DNIE"
    if t in {"PASAPORTE", "PASSPORT"}:
        return "PASAPORTE"
    return t


async def _request(method: str, endpoint: str, base_url: str = BASE_URL, **kwargs):
    """Llama a base_url+endpoint y devuelve (raw, error).
    Si hay error, raw es None y error es el dict _error(...)."""
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            response = await client.request(method, f"{base_url}{endpoint}", **kwargs)
            response.raise_for_status()
    except httpx.TimeoutException:
        return None, _error("TIMEOUT", "El servicio no respondió a tiempo.")
    except httpx.HTTPStatusError as e:
        return None, _error("HTTP_ERROR", f"El servicio devolvió HTTP {e.response.status_code}.")
    except httpx.RequestError as e:
        return None, _error("CONNECTION_ERROR", f"No se pudo conectar al servicio: {e}.")

    try:
        return response.json(), None
    except Exception:
        return response.text, None


def _normalizar(texto: str) -> str:
    """Pasa a minusculas, quita acentos y espacios sobrantes para comparar
    nombres de forma flexible (el ciudadano no escribe con tildes perfectas)."""
    if texto is None:
        return ""
    t = texto.strip().lower()
    t = "".join(
        c for c in unicodedata.normalize("NFD", t)
        if unicodedata.category(c) != "Mn"
    )
    return " ".join(t.split())


# Carga del CSV de codigos INE.
# Nota: el MCP se levanta en cada peticion, por lo que esto se ejecuta
# al importar el modulo. El fichero viaja en el repo (ruta relativa).
def _cargar_codigos():
    filas = []
    try:
        with open(CSV_CODIGOS, encoding="utf-8") as f:
            for r in csv.DictReader(f):
                filas.append({
                    "id_provincia": r["id_provincia"],
                    "provincia":    r["provincia"],
                    "id_localidad": r["id_localidad"],
                    "localidad":    r["localidad"],
                    # campos normalizados para busqueda
                    "_provincia_norm": _normalizar(r["provincia"]),
                    "_localidad_norm": _normalizar(r["localidad"]),
                })
    except FileNotFoundError:
        pass
    return filas


_CODIGOS = _cargar_codigos()


# ──────────────────────────────────────────────
# Tools
# ──────────────────────────────────────────────
def _resolver_localidad(localidad: str):
    """Resuelve un nombre de localidad a su fila de codigos INE.
    Los nombres de localidad son unicos, asi que se busca por coincidencia exacta
    (normalizada: sin tildes ni mayusculas). Devuelve (fila, error): si existe,
    fila es el dict y error None; si no, fila es None y error es un _error(...)."""
    if not _CODIGOS:
        return None, _error("DATA_ERROR", "No se ha podido cargar la tabla de códigos INE.")

    loc_norm = _normalizar(localidad)
    if not loc_norm:
        return None, _error("INVALID_PARAM", "Debe indicar el nombre de la localidad.")

    fila = next((r for r in _CODIGOS if r["_localidad_norm"] == loc_norm), None)
    if fila is None:
        return None, _error("NOT_FOUND", f"No se ha encontrado la localidad '{localidad}'.")

    return {
        "id_provincia": fila["id_provincia"],
        "provincia":    fila["provincia"],
        "id_localidad": fila["id_localidad"],
        "localidad":    fila["localidad"],
    }, None


async def _fetch_comisarias(codigo_peticion: str, localidad: str):
    """Resuelve la localidad y consulta sus comisarias. Devuelve (comisarias, error),
    con comisarias ya numeradas y recortadas a MAX_COMISARIAS. Lo comparten la
    tool de listado y el resolutor por nombre/direccion."""
    fila, err = _resolver_localidad(localidad)
    if err:
        return None, err

    # La API mockeada solo mira idProvincia para acotar la zona, asi que lo
    # enviamos junto a idLocalidad (que la fila de codigos INE ya resuelve).
    params = {
        "codigoPeticion": codigo_peticion,
        "idProvincia":    fila["id_provincia"],
        "idLocalidad":    fila["id_localidad"],
    }

    raw, err = await _request("GET", "/ConsultarComisarias", params=params)
    if err:
        return None, err

    tab = raw.get("tabComi") or [] if isinstance(raw, dict) else []

    comisarias = [
        {
            "numero":       i,
            "id_comisaria": c.get("comisariaCita", ""),
            "nombre":       c.get("desComisariaCita", ""),
            "direccion":    c.get("direccionCita", ""),
        }
        for i, c in enumerate(tab[:MAX_COMISARIAS], start=1)
    ]
    return comisarias, None


async def _resolver_comisaria(codigo_peticion: str, localidad: str, comisaria: str):
    """Resuelve la comisaria elegida por el ciudadano a su id_comisaria interno.

    Acepta el numero de la lista, el nombre o la direccion (sin tildes ni
    mayusculas). Primero busca por subcadena; si no encuentra nada, prueba un
    emparejamiento difuso (rapidfuzz.partial_ratio >= UMBRAL_FUZZY) para tolerar
    erratas. Devuelve (comisaria, error): si hay una unica coincidencia,
    comisaria es el dict {numero, id_comisaria, nombre, direccion} y error None;
    si no hay ninguna o hay varias, comisaria es None y error es un _error(...)
    para que el agente pregunte.
    """
    texto = str(comisaria or "").strip()
    if not texto:
        return None, _error(
            "FALTA_DATO",
            "Falta la comisaria. Preguntasela al ciudadano antes de continuar.",
        )

    comisarias, err = await _fetch_comisarias(codigo_peticion, localidad)
    if err:
        return None, err
    if not comisarias:
        return None, _error("NOT_FOUND", f"No hay comisarias disponibles en '{localidad}'.")

    texto_norm = _normalizar(texto)

    # 1) El ciudadano elige por el numero de la lista.
    if texto_norm.isdigit():
        num = int(texto_norm)
        match = next((c for c in comisarias if c["numero"] == num), None)
        if match:
            return match, None
        return None, _error(
            "NOT_FOUND",
            f"No hay ninguna comisaria con el numero {num} en la lista.",
        )

    # 2) Coincidencia por nombre o direccion (subcadena normalizada).
    coincidencias = [
        c for c in comisarias
        if texto_norm in _normalizar(c["nombre"]) or texto_norm in _normalizar(c["direccion"])
    ]

    # 3) Si la subcadena no encuentra nada, respaldo difuso para tolerar erratas.
    if not coincidencias:
        coincidencias = [
            c for c in comisarias
            if max(
                fuzz.partial_ratio(texto_norm, _normalizar(c["nombre"])),
                fuzz.partial_ratio(texto_norm, _normalizar(c["direccion"])),
            ) >= UMBRAL_FUZZY
        ]

    if len(coincidencias) == 1:
        return coincidencias[0], None
    if not coincidencias:
        return None, _error(
            "NOT_FOUND",
            f"No se ha encontrado ninguna comisaria que coincida con '{comisaria}'.",
        )

    listado = "\n".join(
        f"{c['numero']}. {c['nombre']} - {c['direccion']}" for c in coincidencias
    )
    return None, _error(
        "AMBIGUO",
        f"Hay varias comisarias que coinciden con '{comisaria}':\n{listado}\n"
        "Pide al ciudadano que concrete cual.",
    )


@mcp.tool()
def validar_documento(numero_documento: str) -> dict:
    """
    Valida un número de documento español (DNI o NIE) comprobando su formato y
    su letra de control. Úsala para verificar el documento que da el ciudadano
    antes de consultar, dar de alta, modificar o anular una cita: así el agente
    no tiene que validar el número por su cuenta. Si el documento no es válido,
    pídeselo de nuevo al ciudadano.

    Args:
        numero_documento: Número de documento a validar (DNI o NIE).

    Devuelve el tipo detectado ('DNI' o 'NIE') y el número normalizado. Si no es
    válido, devuelve un error para que lo pidas de nuevo.
    """
    doc = str(numero_documento or "").strip().upper().replace("-", "").replace(" ", "")
    if not doc:
        return _error(
            "FALTA_DATO",
            "Falta el numero de documento. Preguntaselo al ciudadano antes de continuar.",
        )

    # NIE: X/Y/Z + 7 digitos + letra de control. La letra inicial se sustituye
    # por su digito (X->0, Y->1, Z->2) antes de calcular la letra de control.
    if doc[0] in {"X", "Y", "Z"}:
        cuerpo, letra = doc[:-1], doc[-1]
        numero = {"X": "0", "Y": "1", "Z": "2"}[cuerpo[0]] + cuerpo[1:]
        if len(cuerpo) != 8 or not numero.isdigit() or not letra.isalpha():
            return _error(
                "DOCUMENTO_INVALIDO",
                f"El NIE '{numero_documento}' no tiene un formato valido. "
                "Pideselo de nuevo al ciudadano y verificalo.",
            )
        if _LETRAS_CONTROL[int(numero) % 23] != letra:
            return _error(
                "DOCUMENTO_INVALIDO",
                f"La letra del NIE '{numero_documento}' no es correcta. "
                "Pideselo de nuevo al ciudadano y verificalo.",
            )
        tipo = "NIE"

    # DNI: 8 digitos + letra de control.
    elif doc[0].isdigit():
        cuerpo, letra = doc[:-1], doc[-1]
        if len(cuerpo) != 8 or not cuerpo.isdigit() or not letra.isalpha():
            return _error(
                "DOCUMENTO_INVALIDO",
                f"El DNI '{numero_documento}' no tiene un formato valido. "
                "Pideselo de nuevo al ciudadano y verificalo.",
            )
        if _LETRAS_CONTROL[int(cuerpo) % 23] != letra:
            return _error(
                "DOCUMENTO_INVALIDO",
                f"La letra del DNI '{numero_documento}' no es correcta. "
                "Pideselo de nuevo al ciudadano y verificalo.",
            )
        tipo = "DNI"

    else:
        return _error(
            "DOCUMENTO_INVALIDO",
            f"El documento '{numero_documento}' no es un DNI ni un NIE valido. "
            "Pideselo de nuevo al ciudadano y verificalo.",
        )

    # Red de seguridad: rechazamos numeros de ejemplo/inventados (todos iguales o
    # secuenciales, p. ej. 12345678Z) aunque el checksum sea valido. El modelo
    # los fabrica cuando no tiene el documento real; obligamos a pedir el de verdad.
    digitos = cuerpo[1:] if tipo == "NIE" else cuerpo
    if _es_documento_ejemplo(digitos):
        return _error(
            "DOCUMENTO_INVALIDO",
            f"El documento '{numero_documento}' parece un numero de ejemplo, no uno "
            "real. No uses documentos de ejemplo: pideselo de nuevo al ciudadano.",
        )

    return {
        "ok": True,
        "data": {
            "valido":           True,
            "tipo":             tipo,
            "numero_documento": doc,
            "resumen_texto":    f"El {tipo} {doc} es valido.",
        },
    }


@mcp.tool()
async def consultar_comisarias(
    codigo_peticion: str,
    localidad: str,
) -> dict:
    """
    Lista las comisarías disponibles para tramitar DNI/NIE/pasaporte en una localidad.

    Args:
        codigo_peticion: Identificador de la petición.
        localidad: Nombre de la localidad.

    Devuelve "listado_texto" (ya numerado, se muestra tal cual) y "comisarias".
    Para reservar, el ciudadano elige por número, nombre o dirección; las tools
    de slots/alta/modificar resuelven la comisaría por su cuenta.
    """
    comisarias, err = await _fetch_comisarias(codigo_peticion, localidad)
    if err:
        return err

    # Lista ya escrita y numerada, lista para mostrar al usuario tal cual.
    # Usamos guion ASCII normal (no raya larga) para que el modelo cuantizado
    # pueda copiarla literal sin atascarse con caracteres raros.
    listado_texto = "\n".join(
        f"{c['numero']}. {c['nombre']} - {c['direccion']}" for c in comisarias
    )

    # No exponemos id_comisaria al agente para que no lo confunda: para reservar
    # basta con numero/nombre/direccion, y slots/alta/modificar resuelven el id
    # por su cuenta a partir de eso.
    publicas = [
        {"numero": c["numero"], "nombre": c["nombre"], "direccion": c["direccion"]}
        for c in comisarias
    ]

    return {
        "ok": True,
        "data": {
            "comisarias":    publicas,
            "listado_texto": listado_texto,
        },
    }


def _parsear_cita(raw):
    """Extrae de la respuesta cruda de Consultar/Alta los campos de la cita ya
    formateados. Devuelve (tiene_cita, cita_dict): si no hay fecha real,
    tiene_cita es False y cita_dict es None."""
    datos     = raw if isinstance(raw, dict) else {}
    fecha_raw = str(datos.get("fechaCita", "") or "").strip()
    hora_raw  = str(datos.get("horaCita", "") or "").strip()

    if not fecha_raw:
        return False, None

    fecha_disp, hora_disp = _display_fecha_hora(fecha_raw, hora_raw)
    cita = {
        "fecha":       fecha_disp,
        "hora":        hora_disp,
        "tramite":     str(datos.get("tramiteCita", "") or "").strip(),
        "comisaria":   str(datos.get("desComisariaCita", "") or "").strip(),
        "direccion":   str(datos.get("direccionCita", "") or "").strip(),
        "numero_cita": str(datos.get("numCita", "") or "").strip(),
    }
    return True, cita


def _frase_cita(cita: dict) -> str:
    """Frase con los datos de una cita ya formateada: 'el DD/MM/YYYY a las HH:MM
    en COMISARIA (DIRECCION)'. Omite las partes que falten."""
    frase = f"el {cita['fecha']} a las {cita['hora']}"
    if cita.get("comisaria"):
        frase += f" en {cita['comisaria']}"
    if cita.get("direccion"):
        frase += f" ({cita['direccion']})"
    return frase


async def _obtener_cita(codigo_peticion: str, tipo_documento: str, numero_documento: str):
    """Consulta la cita del titular y devuelve (data, error).

    data tiene el formato: tipo_documento, numero_documento, tiene_cita (bool),
    cita (dict|None) y resumen_texto. Lo usan tanto la tool de consulta como
    alta/anular/modificar para autocomprobarse antes de actuar.
    """
    if not numero_documento or not numero_documento.strip():
        return None, _error(
            "FALTA_DATO",
            "Falta el numero de documento. Preguntaselo al ciudadano antes de continuar.",
        )

    params = {
        "codigoPeticion": codigo_peticion,
        "tipotitular":    tipo_documento,
        "Idtitular":      numero_documento,
    }

    raw, err = await _request("GET", "/ConsultarCitaDnie", params=params)
    if err:
        return None, err

    tiene_cita, cita = _parsear_cita(raw)
    if tiene_cita:
        resumen_texto = f"Cita {_frase_cita(cita)}."
        if cita["numero_cita"]:
            resumen_texto += f" Numero de cita: {cita['numero_cita']}."
    else:
        resumen_texto = "No consta ninguna cita para este documento."

    data = {
        "tipo_documento":   tipo_documento,
        "numero_documento": numero_documento,
        "tiene_cita":       tiene_cita,
        "cita":             cita,
        "resumen_texto":    resumen_texto,
    }
    return data, None




@mcp.tool()
async def consultar_cita_dnie(
    codigo_peticion: str,
    numero_documento: str,
) -> dict:
    """
    Consulta la cita de DNI/NIE/pasaporte de un titular. Úsala solo cuando el
    ciudadano pregunta expresamente si tiene cita; el resto de tools ya
    comprueban la cita por su cuenta.

    Args:
        codigo_peticion: Identificador de la petición.
        numero_documento: Número de documento (DNI o NIE); el tipo se deduce solo.
    """
    tipo_documento, err = _inferir_tipo_documento(numero_documento)
    if err:
        return err

    data, err = await _obtener_cita(codigo_peticion, tipo_documento, numero_documento)
    if err:
        return err

    return {"ok": True, "data": data}



def _build_alta_body(codigo_peticion, tipo_documento, numero_documento,
                      id_comisaria, fecha_cita, hora_cita, id_tramite=""):
    body = {
        "codigoPeticion": codigo_peticion,
        "tipotitular":    tipo_documento,
        "idtitular":      numero_documento,
        "idComisaria":    id_comisaria,
        "fechaCita":      fecha_cita,
        "horaCita":       hora_cita,
    }
    if id_tramite:
        body["idTramite"] = id_tramite
    return body


@mcp.tool()
async def alta_cita_dnie(
    codigo_peticion: str,
    numero_documento: str,
    localidad: str,
    comisaria: str,
    fechaCita: str,
    horaCita: str,
    id_tramite: str = "",
) -> dict:
    """
    Da de alta una cita de DNI/NIE/pasaporte. Antes comprueba si el titular ya
    tiene una cita: si la tiene, informa y NO crea otra.

    Args:
        codigo_peticion: Identificador de la petición.
        numero_documento: Número de documento (DNI o NIE); el tipo se deduce solo.
        localidad: Localidad de la comisaría.
        comisaria: Comisaría elegida por el ciudadano (número de la lista, nombre
            o dirección); la tool resuelve el código por su cuenta.
        fechaCita: Fecha de la cita. Formato AAAAMMDD.
        horaCita: Hora de la cita. Formato HHMM.
        id_tramite: Opcional: 'DNI', 'NIE' o 'PASAPORTE' (mayúsculas o minúsculas);
            la tool convierte DNI/NIE a 'DNIE' para la API.
    """
    tipo_documento, err = _inferir_tipo_documento(numero_documento)
    if err:
        return err

    err = _validar_requeridos(localidad=localidad, comisaria=comisaria,
                              fechaCita=fechaCita, horaCita=horaCita)
    if err:
        return err

    # Autocomprobación: si ya hay cita, informamos y no creamos otra.
    cita_actual, err = await _obtener_cita(codigo_peticion, tipo_documento, numero_documento)
    if err:
        return err
    if cita_actual["tiene_cita"]:
        return {
            "ok": True,
            "data": {
                "creada":        False,
                "tiene_cita":    True,
                "cita":          cita_actual["cita"],
                "resumen_texto": "El titular ya tiene una cita; no se ha creado otra. "
                                 + cita_actual["resumen_texto"],
            },
        }

    # Resolvemos la comisaría (número/nombre/dirección) a su código interno.
    comisaria_resuelta, err = await _resolver_comisaria(codigo_peticion, localidad, comisaria)
    if err:
        return err
    id_comisaria = comisaria_resuelta["id_comisaria"]

    # El agente pasa 'DNI'/'NIE'/'PASAPORTE'; la API espera 'DNIE'/'PASAPORTE'.
    id_tramite = _normalizar_id_tramite(id_tramite)

    body = _build_alta_body(codigo_peticion, tipo_documento, numero_documento,
                             id_comisaria, fechaCita, horaCita, id_tramite)

    raw, err = await _request("POST", "/AltaCitaDnie", json=body)
    if err:
        return err

    return {
        "ok": True,
        "data": {
            "creada":        True,
            "resumen_texto": "Cita creada correctamente.",
        },
    }


@mcp.tool()
async def anular_cita_dnie(
    codigo_peticion: str,
    numero_documento: str,
) -> dict:
    """
    Anula la cita de DNI/NIE/pasaporte de un titular. Antes comprueba si existe
    la cita: si no hay ninguna, informa y no llama al servicio.

    Args:
        codigo_peticion: Identificador de la petición.
        numero_documento: Número de documento (DNI o NIE); el tipo se deduce solo.
    """
    tipo_documento, err = _inferir_tipo_documento(numero_documento)
    if err:
        return err

    # Autocomprobación: si no hay cita, no hay nada que anular.
    cita_actual, err = await _obtener_cita(codigo_peticion, tipo_documento, numero_documento)
    if err:
        return err
    if not cita_actual["tiene_cita"]:
        return {
            "ok": True,
            "data": {
                "anulada":       False,
                "resumen_texto": "No consta ninguna cita para este documento; no hay nada que anular.",
            },
        }

    body = {
        "codigoPeticion": codigo_peticion,
        "tipotitular":    tipo_documento,
        "Idtitular":      numero_documento,
    }

    raw, err = await _request("PUT", "/AnularCitaDnie", json=body)
    if err:
        return err

    return {
        "ok": True,
        "data": {
            "anulada":       True,
            "resumen_texto": "Cita anulada correctamente.",
        },
    }


@mcp.tool()
async def modificar_cita_dnie(
    codigo_peticion: str,
    numero_documento: str,
    localidad: str,
    comisaria: str,
    fechaCita: str,
    horaCita: str,
    id_tramite: str = "",
) -> dict:
    """
    Modifica la cita de un titular. Comprueba si ya tiene cita: si la tiene, la
    anula y crea la nueva; si no la tiene, crea directamente la nueva.

    Args:
        codigo_peticion: Identificador de la petición.
        numero_documento: Número de documento (DNI o NIE); el tipo se deduce solo.
        localidad: Localidad de la comisaría.
        comisaria: Comisaría elegida por el ciudadano (número de la lista, nombre
            o dirección); la tool resuelve el código por su cuenta.
        fechaCita: Fecha de la nueva cita. Formato AAAAMMDD.
        horaCita: Hora de la nueva cita. Formato HHMM.
        id_tramite: Opcional: 'DNI', 'NIE' o 'PASAPORTE' (mayúsculas o minúsculas);
            la tool convierte DNI/NIE a 'DNIE' para la API.
    """
    tipo_documento, err = _inferir_tipo_documento(numero_documento)
    if err:
        return err

    err = _validar_requeridos(localidad=localidad, comisaria=comisaria,
                              fechaCita=fechaCita, horaCita=horaCita)
    if err:
        return err

    # Autocomprobación: solo anulamos si de verdad hay una cita previa.
    cita_actual, err = await _obtener_cita(codigo_peticion, tipo_documento, numero_documento)
    if err:
        return err
    tenia_cita = cita_actual["tiene_cita"]

    # Resolvemos la comisaría antes de anular, para no dejar al titular sin cita
    # si la comisaría indicada no existe.
    comisaria_resuelta, err = await _resolver_comisaria(codigo_peticion, localidad, comisaria)
    if err:
        return err
    id_comisaria = comisaria_resuelta["id_comisaria"]

    if tenia_cita:
        body_anular = {
            "codigoPeticion": codigo_peticion,
            "tipotitular":    tipo_documento,
            "Idtitular":      numero_documento,
        }
        raw_anular, err = await _request("PUT", "/AnularCitaDnie", json=body_anular)
        if err:
            return err

    # El agente pasa 'DNI'/'NIE'/'PASAPORTE'; la API espera 'DNIE'/'PASAPORTE'.
    id_tramite = _normalizar_id_tramite(id_tramite)

    body_alta = _build_alta_body(codigo_peticion, tipo_documento, numero_documento,
                                  id_comisaria, fechaCita, horaCita, id_tramite)
    raw_alta, err = await _request("POST", "/AltaCitaDnie", json=body_alta)
    if err:
        return err

    if tenia_cita:
        resumen = "Cita modificada correctamente."
    else:
        resumen = "No había cita previa; se ha creado una nueva cita."

    return {
        "ok": True,
        "data": {
            "modificada":    tenia_cita,
            "creada":        not tenia_cita,
            "resumen_texto": resumen,
        },
    }


@mcp.tool()
async def enviar_sms(
    codigo_peticion: str,
    destinatario: str,
    localidad: str,
    comisaria: str,
    fechaCita: str,
    horaCita: str,
    tramite: str = "",
) -> dict:
    """
    Envía un SMS de confirmación de cita. La tool construye el mensaje con un
    formato fijo a partir de los datos de la cita: resuelve la comisaría para
    poner su nombre y dirección oficiales, formatea la fecha y la hora, y añade
    el prefijo +34 al móvil.

    Args:
        codigo_peticion: Identificador de la petición.
        destinatario: Móvil de 9 dígitos, sin prefijo.
        localidad: Localidad de la comisaría.
        comisaria: Comisaría elegida por el ciudadano (número de la lista, nombre
            o dirección); la tool resuelve nombre y dirección por su cuenta.
        fechaCita: Fecha de la cita. Formato AAAAMMDD.
        horaCita: Hora de la cita. Formato HHMM.
        tramite: Trámite de la cita (DNI, NIE o Pasaporte).
    """
    err = _validar_requeridos(destinatario=destinatario, localidad=localidad,
                              comisaria=comisaria, fechaCita=fechaCita, horaCita=horaCita)
    if err:
        return err

    # Resolvemos la comisaría para poner en el SMS su nombre y dirección reales,
    # no lo que haya tecleado el modelo.
    comisaria_resuelta, err = await _resolver_comisaria(codigo_peticion, localidad, comisaria)
    if err:
        return err

    fecha_disp, hora_disp = _display_fecha_hora(fechaCita, horaCita)

    # Mensaje con formato fijo, construido siempre igual dentro de la tool.
    tramite_txt = str(tramite or "").strip()
    encabezado = f"Cita confirmada para {tramite_txt}" if tramite_txt else "Cita confirmada"
    mensaje = (
        f"{encabezado}: el {fecha_disp} a las {hora_disp} en "
        f"{comisaria_resuelta['nombre']} ({comisaria_resuelta['direccion']}). "
        "Acuda con su documentación."
    )

    # Inyectamos el prefijo +34 dentro de la tool. Aceptamos que llegue ya con
    # prefijo (+34... o 34...) para no duplicarlo.
    numero = str(destinatario).strip().replace(" ", "")
    if numero.startswith("+"):
        telefono = numero
    elif numero.startswith("34"):
        telefono = f"+{numero}"
    else:
        telefono = f"+34{numero}"

    body = {"to": telefono, "message": mensaje}

    raw, err = await _request("POST", "/sms/send", json=body)
    if err:
        return err

    return {
        "ok": True,
        "data": {
            "enviado":       True,
            "mensaje":       mensaje,
            "resumen_texto": "SMS enviado.",
        },
    }


def _parse_slot(start_time: str):
    """De un startTime ISO devuelve (fechaCita 'AAAAMMDD', horaCita 'HHMM', display)."""
    try:
        dt = datetime.fromisoformat(str(start_time).replace("Z", "").strip())
        return dt.strftime("%Y%m%d"), dt.strftime("%H%M"), dt.strftime("%d/%m/%Y %H:%M")
    except Exception:
        return "", "", str(start_time)


def _fecha_a_iso(aaaammdd: str) -> str:
    """Convierte una fecha 'AAAAMMDD' a 'YYYY-MM-DD' (formato que espera la API
    de slots). Si ya viene en otro formato, la devuelve tal cual."""
    s = str(aaaammdd).strip()
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return s


def _display_fecha_hora(fecha: str, hora: str):
    """Formatea fechaCita 'AAAAMMDD' -> 'DD/MM/YYYY' y horaCita 'HHMM' -> 'HH:MM'.
    Si no encajan en ese formato, devuelve el valor original sin tocar."""
    f = str(fecha or "").strip()
    h = str(hora or "").strip()
    fecha_disp = f"{f[6:8]}/{f[4:6]}/{f[:4]}" if len(f) == 8 and f.isdigit() else f
    hora_disp  = f"{h[:2]}:{h[2:4]}" if len(h) == 4 and h.isdigit() else h
    return fecha_disp, hora_disp



@mcp.tool()
async def consultar_slots_comisaria(
    codigo_peticion: str,
    localidad: str,
    comisaria: str,
    start_date: str,
) -> dict:
    """
    Devuelve los huecos disponibles de una comisaría para un día.

    Args:
        codigo_peticion: Identificador de la petición.
        localidad: Localidad de la comisaría.
        comisaria: Comisaría elegida por el ciudadano (número de la lista, nombre
            o dirección); la tool resuelve el código por su cuenta.
        start_date: Día de la cita. Formato AAAAMMDD.

    Devuelve "listado_texto" (ya numerado, se muestra tal cual) y "slots"
    (mapea el número elegido a su fechaCita y horaCita).
    """
    err = _validar_requeridos(localidad=localidad, comisaria=comisaria, start_date=start_date)
    if err:
        return err

    comisaria_resuelta, err = await _resolver_comisaria(codigo_peticion, localidad, comisaria)
    if err:
        return err
    id_comisaria = comisaria_resuelta["id_comisaria"]

    fecha_iso = _fecha_a_iso(start_date)
    params = {
        "startDate": fecha_iso,
        "endDate": fecha_iso,
    }

    raw, err = await _request("GET", f"/offices/{id_comisaria}/slots", params=params)
    if err:
        return err

    todos = raw.get("slots") or [] if isinstance(raw, dict) else []

    # La API mockeada devuelve todos los slots; el agente solo necesita los
    # que tienen available=true.
    disponibles = [s for s in todos if s.get("available") is True]

    slots = []
    for i, s in enumerate(disponibles, start=1):
        fecha, hora, cuando = _parse_slot(s.get("startTime", ""))
        slots.append({
            "numero":    i,
            "fechaCita": fecha,
            "horaCita":  hora,
            "cuando":    cuando,
        })

    listado_texto = "\n".join(f"{x['numero']}. {x['cuando']}" for x in slots)

    return {
        "ok": True,
        "data": {
            "slots":         slots,
            "listado_texto": listado_texto,
        },
    }




@mcp.tool()
def crear_codigo_peticion() -> dict:
    """
    Genera un código de petición alfanumérico aleatorio.
    """
    alfabeto = string.ascii_letters + string.digits
    codigo = "".join(secrets.choice(alfabeto) for _ in range(20))
    return {"ok": True, "data": {"codigo": codigo}}



# ──────────────────────────────────────────────
 # Arranque
 # ──────────────────────────────────────────────
def main():
    mcp.run(transport="stdio")
 
 
if __name__ == "__main__":
    main()




 
