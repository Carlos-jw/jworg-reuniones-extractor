"""
EXTRACTOR DE REUNIONES JW.ORG - VERSIÓN GOOGLE COLAB
Autor: Asistente Claude
Fecha: 2025

INSTRUCCIONES:
1. Ejecuta este código en Google Colab
2. Sigue las instrucciones en pantalla
3. Selecciona opción 2 (Google Sheets) cuando preguntes
"""

import requests
from bs4 import BeautifulSoup
import re
import json
from typing import Optional, Dict, List, Tuple
from datetime import datetime
import pandas as pd

# Para Google Colab (método simple)
try:
    from google.colab import auth, files
    import gspread
    from google.auth import default
    IN_COLAB = True
    SHEETS_DISPONIBLE = True
    print("✅ Ejecutando en Google Colab")
except ImportError:
    IN_COLAB = False
    SHEETS_DISPONIBLE = False
    print("⚠️ No estás en Colab. Usa el método de Service Account")

# ==================== CONFIGURACIÓN ====================
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'es-ES,es;q=0.9',
    'Connection': 'keep-alive'
}

TIMEOUT = 30
MAX_REINTENTOS = 3

LIBROS_BIBLIA = (
    'ECLESIASTÉS', 'GÉNESIS', 'ÉXODO', 'LEVÍTICO', 'NÚMEROS', 'DEUTERONOMIO',
    'JOSUÉ', 'JUECES', 'RUT', 'SAMUEL', 'REYES', 'CRÓNICAS', 'ESDRAS',
    'NEHEMÍAS', 'ESTER', 'JOB', 'SALMOS', 'PROVERBIOS', 'CANTARES',
    'ISAÍAS', 'JEREMÍAS', 'LAMENTACIONES', 'EZEQUIEL', 'DANIEL',
    'OSEAS', 'JOEL', 'AMÓS', 'ABDÍAS', 'JONÁS', 'MIQUEAS', 'NAHÚM',
    'HABACUC', 'SOFONÍAS', 'HAGEO', 'ZACARÍAS', 'MALAQUÍAS',
    'MATEO', 'MARCOS', 'LUCAS', 'JUAN', 'HECHOS', 'ROMANOS',
    'CORINTIOS', 'GÁLATAS', 'EFESIOS', 'FILIPENSES', 'COLOSENSES',
    'TESALONICENSES', 'TIMOTEO', 'TITO', 'FILEMÓN', 'HEBREOS',
    'SANTIAGO', 'PEDRO', 'JUDAS', 'APOCALIPSIS'
)

PATRONES = {
    'fecha': re.compile(r'\d{1,2}\s*(?:-\s*\d{1,2}|de\s+\w+\s+(?:a|al)\s+\d{1,2})\s+de\s+\w+', re.IGNORECASE),
    'cancion': re.compile(r'Canción\s+(\d+)', re.IGNORECASE),
    'palabras': re.compile(r'Palabras\s+de\s+(introducción|conclusión)\s*[:\(]?\s*(\d+)\s*min', re.IGNORECASE),
    'parte_numerada': re.compile(r'^(\d+)\.\s*([^\n(]+?)\s*\((\d+)\s*min', re.MULTILINE | re.IGNORECASE),
    'parte_sin_numero': re.compile(r'^(Empiece conversaciones|Haga revisitas|Estudio bíblico|Necesidades de la congregación|Canción del Reino y oración final)\s*\(?\s*(\d+)\s*min', re.MULTILINE | re.IGNORECASE)
}

# ==================== AUTENTICACIÓN SIMPLE PARA COLAB ====================
def conectar_google_sheets():
    """Conecta con Google Sheets usando la autenticación de Colab."""
    if not SHEETS_DISPONIBLE:
        print("❌ Este código debe ejecutarse en Google Colab")
        print("   Ve a: https://colab.research.google.com/")
        return None
    
    try:
        print("\n" + "="*60)
        print("🔐 AUTENTICACIÓN CON GOOGLE")
        print("="*60)
        print("\n📋 Pasos:")
        print("1. Se abrirá una ventana de permisos")
        print("2. Selecciona tu cuenta Gmail")
        print("3. Click en 'Permitir'")
        print("\n⏳ Autenticando...\n")
        
        # Autenticar con Colab (abre ventana automáticamente)
        auth.authenticate_user()
        
        # Obtener credenciales
        creds, _ = default()
        gc = gspread.authorize(creds)
        
        print("✅ ¡AUTENTICACIÓN EXITOSA!")
        print("="*60 + "\n")
        
        return gc
        
    except Exception as e:
        print(f"\n❌ Error de autenticación: {e}")
        print("\n💡 Solución:")
        print("   1. Reinicia el runtime: Runtime → Restart runtime")
        print("   2. Vuelve a ejecutar el código")
        return None

# ==================== FUNCIONES DE EXTRACCIÓN ====================
def obtener_enlaces_semanas(url_indice: str) -> List[Dict[str, str]]:
    """Extrae todos los enlaces de semanas desde la URL índice."""
    try:
        print("🔍 Buscando todas las semanas disponibles...\n")
        response = requests.get(url_indice, headers=HEADERS, timeout=TIMEOUT)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        enlaces = []
        
        main_content_div = soup.find('div', class_='docPart')
        if main_content_div:
            links = main_content_div.find_all('a', href=True)
        else:
            links = soup.find_all('a', href=True)
        
        for link in links:
            href = link.get('href')
            texto = link.get_text(strip=True)
            
            if '/es/biblioteca/guia-actividades-reunion-testigos-jehova/' in href and texto:
                if href != url_indice and not href.endswith('/mwb/'):
                    if PATRONES['fecha'].search(texto):
                        if not href.startswith('http'):
                            href = f"https://www.jw.org{href}"
                        enlaces.append({'titulo': texto, 'url': href})
        
        enlaces.sort(key=lambda x: extraer_fecha_para_ordenar(x['titulo']))
        
        print(f"✅ Se encontraron {len(enlaces)} semanas\n")
        return enlaces
        
    except Exception as e:
        print(f"❌ Error al obtener enlaces: {e}")
        return []

def extraer_fecha_para_ordenar(titulo: str) -> tuple:
    """Extrae la fecha inicial para ordenar cronológicamente."""
    meses = {
        'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4,
        'mayo': 5, 'junio': 6, 'julio': 7, 'agosto': 8,
        'septiembre': 9, 'octubre': 10, 'noviembre': 11, 'diciembre': 12
    }
    
    match = re.search(r'(\d{1,2})[- ].*?de\s+(\w+)', titulo, re.IGNORECASE)
    if match:
        dia = int(match.group(1))
        mes_texto = match.group(2).lower()
        mes = meses.get(mes_texto, 0)
        return (mes, dia)
    return (0, 0)

def mostrar_semanas_disponibles(enlaces: List[Dict[str, str]]) -> None:
    """Muestra las semanas disponibles."""
    if not enlaces:
        print("❌ No se encontraron semanas")
        return
    
    print("\n" + "="*60)
    print("📅 SEMANAS DISPONIBLES")
    print("="*60 + "\n")
    for i, sem in enumerate(enlaces, 1):
        print(f"  {i:2d}. {sem['titulo']}")
    print()

def obtener_contenido(url: str) -> Optional[str]:
    """Descarga y extrae texto de la página web con reintentos."""
    for intento in range(1, MAX_REINTENTOS + 1):
        try:
            response = requests.get(
                url,
                headers=HEADERS,
                timeout=TIMEOUT,
                allow_redirects=True
            )
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            main = soup.find('main') or soup
            return main.get_text(separator='\n', strip=True)
        except requests.Timeout:
            if intento == MAX_REINTENTOS:
                print(f"⏱️ Timeout")
        except requests.RequestException as e:
            if intento == MAX_REINTENTOS:
                print(f"❌ Error: {e}")
    return None

def extraer_fecha_correcta(contenido: str) -> str:
    """Extrae la fecha correcta del contenido."""
    lineas = contenido.split('\n')
    
    for linea in lineas[:20]:
        fecha_match = PATRONES['fecha'].search(linea)
        if fecha_match:
            return fecha_match.group(0).strip()
    
    fecha_match = PATRONES['fecha'].search(contenido)
    if fecha_match:
        return fecha_match.group(0).strip()
    
    return ''

def extraer_lectura_biblica(contenido: str) -> str:
    """Extrae la lectura bíblica del contenido."""
    for libro in LIBROS_BIBLIA:
        patron = rf'({libro})\s*\d+(?::\d+)?(?:[-–]\d+(?::\d+)?)?'
        match = re.search(patron, contenido, re.IGNORECASE | re.DOTALL)
        if match:
            return re.sub(r'\s+', ' ', match.group(0)).strip()
    
    match2 = re.search(
        r'Lectura\s+b[ií]blica\s*[:\-]?\s*([A-Za-zÁÉÍÓÚáéíóúñÑ0-9\s:–\-]+)',
        contenido, re.IGNORECASE
    )
    return re.sub(r'\s+', ' ', match2.group(1)).strip() if match2 else ''

def extraer_canciones(contenido: str) -> Dict[str, str]:
    """Extrae números de las 3 canciones."""
    nums = PATRONES['cancion'].findall(contenido)
    return {
        'cancion_inicial': f"Canción {nums[0]}" if len(nums) > 0 else '',
        'cancion_intermedia': f"Canción {nums[1]}" if len(nums) > 1 else '',
        'cancion_final': f"Canción {nums[2]}" if len(nums) > 2 else ''
    }

def extraer_palabras(contenido: str) -> Dict[str, str]:
    """Extrae palabras de introducción y conclusión."""
    palabras = {}
    for match in PATRONES['palabras'].finditer(contenido):
        tipo, mins = match.groups()
        palabras[f'palabras_{tipo}'] = f"Palabras de {tipo} ({mins} min)"
    
    return {
        'palabras_introduccion': palabras.get('palabras_introducción', ''),
        'palabras_conclusion': palabras.get('palabras_conclusión', '')
    }

def encontrar_posicion_cancion_intermedia(contenido: str) -> int:
    """Encuentra después de qué número de parte viene la canción intermedia."""
    canciones = list(PATRONES['cancion'].finditer(contenido))
    if len(canciones) < 2:
        return 6
    
    pos_cancion = canciones[1].start()
    partes_antes = []
    
    for match in PATRONES['parte_numerada'].finditer(contenido):
        if match.start() < pos_cancion:
            partes_antes.append(int(match.group(1)))
    
    return max(partes_antes) if partes_antes else 6

def extraer_partes(contenido: str) -> Tuple[Dict[str, List[Dict]], int]:
    """Extrae y clasifica partes dinámicamente según posición de canciones."""
    parte_antes_cancion = encontrar_posicion_cancion_intermedia(contenido)
    
    secciones = {
        'tesoros_biblia': [],
        'seamos_maestros': [],
        'vida_cristiana': []
    }
    
    contador_parte = 0
    
    for match in PATRONES['parte_numerada'].finditer(contenido):
        num = int(match.group(1))
        contador_parte += 1
        parte = {
            'numero': contador_parte,
            'titulo': match.group(2).strip(),
            'duracion': f"{match.group(3)} min"
        }
        
        if num <= 3:
            secciones['tesoros_biblia'].append(parte)
        elif num <= parte_antes_cancion:
            secciones['seamos_maestros'].append(parte)
        else:
            secciones['vida_cristiana'].append(parte)
    
    for match in PATRONES['parte_sin_numero'].finditer(contenido):
        contador_parte += 1
        parte = {
            'numero': contador_parte,
            'titulo': match.group(1).strip(),
            'duracion': f"{match.group(2)} min"
        }
        secciones['vida_cristiana'].append(parte)
    
    return secciones, parte_antes_cancion

def extraer_datos_reunion(url: str) -> Optional[Dict]:
    """Extrae todos los datos de la reunión desde la URL."""
    contenido = obtener_contenido(url)
    if not contenido:
        return None
    
    partes_data, corte = extraer_partes(contenido)
    
    datos = {
        'fecha': extraer_fecha_correcta(contenido),
        'lectura_biblica': extraer_lectura_biblica(contenido),
        **extraer_canciones(contenido),
        **extraer_palabras(contenido),
        **partes_data,
        '_corte_cancion': corte
    }
    
    return datos

# ==================== FUNCIONES PARA GOOGLE SHEETS ====================
def crear_plantilla_sheets(gc, titulo_libro: str) -> Optional[str]:
    """Crea una nueva hoja de cálculo con la plantilla."""
    try:
        print(f"\n📝 Creando hoja de cálculo: '{titulo_libro}'...")
        
        sh = gc.create(titulo_libro)
        worksheet = sh.sheet1
        worksheet.update_title("Reuniones")
        
        encabezados = [
            'Semana', 'Fecha', 'Lectura Bíblica',
            'Canción Inicial', 'Palabras Introducción',
        ]
        
        for i in range(1, 10):
            encabezados.extend([f'Parte {i}', f'Duración {i}'])
        
        encabezados.extend([
            'Canción Intermedia', 'Palabras Conclusión', 'Canción Final'
        ])
        
        worksheet.append_row(encabezados)
        
        worksheet.format('1:1', {
            'backgroundColor': {'red': 0.2, 'green': 0.2, 'blue': 0.6},
            'textFormat': {'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}, 'bold': True}
        })
        
        worksheet.freeze(rows=1)
        sh.share('', perm_type='anyone', role='reader')
        
        print(f"✅ Hoja creada exitosamente")
        print(f"\n🔗 ENLACE: {sh.url}\n")
        
        return sh.id
        
    except Exception as e:
        print(f"❌ Error al crear plantilla: {e}")
        return None

def rellenar_sheets(gc, spreadsheet_id: str, datos_lista: List[Dict]) -> None:
    """Rellena la plantilla de Sheets con los datos."""
    try:
        print("\n" + "="*60)
        print("📥 RELLENANDO GOOGLE SHEETS")
        print("="*60 + "\n")
        
        sh = gc.open_by_key(spreadsheet_id)
        worksheet = sh.sheet1
        
        total = len(datos_lista)
        
        for i, datos in enumerate(datos_lista, 1):
            partes = [
                *datos['tesoros_biblia'],
                *datos['seamos_maestros'],
                *datos['vida_cristiana']
            ]
            partes.sort(key=lambda x: x['numero'])
            
            fila = [
                i,
                datos['fecha'],
                datos['lectura_biblica'],
                datos['cancion_inicial'],
                datos['palabras_introduccion'],
            ]
            
            for j in range(9):
                if j < len(partes):
                    fila.append(partes[j]['titulo'])
                    fila.append(partes[j]['duracion'])
                else:
                    fila.append('')
                    fila.append('')
            
            fila.extend([
                datos['cancion_intermedia'],
                datos['palabras_conclusion'],
                datos['cancion_final']
            ])
            
            worksheet.append_row(fila)
            
            # Mostrar progreso
            print(f"  ✅ Semana {i}/{total}: {datos['fecha']}")
        
        print(f"\n✅ ¡COMPLETADO! {total} semanas añadidas")
        print(f"\n📊 Accede a tu hoja aquí: {sh.url}\n")
        
    except Exception as e:
        print(f"❌ Error al rellenar Sheets: {e}")

# ==================== FUNCIÓN PRINCIPAL ====================
def main():
    """Función principal."""
    print("\n" + "="*60)
    print("🚀 EXTRACTOR DE REUNIONES JW.ORG")
    print("   Versión Google Colab")
    print("="*60 + "\n")
    
    if not IN_COLAB:
        print("❌ Este código debe ejecutarse en Google Colab")
        print("   Ve a: https://colab.research.google.com/")
        return
    
    # URL del programa
    print("📌 Ingresa la URL del programa (ejemplo):")
    print("   https://www.jw.org/es/biblioteca/guia-actividades-reunion-testigos-jehova/\n")
    
    URL_INDICE = input("🔗 URL: ").strip()
    
    if not URL_INDICE:
        print("❌ Debes ingresar una URL")
        return
    
    # Obtener enlaces
    enlaces = obtener_enlaces_semanas(URL_INDICE)
    
    if not enlaces:
        print("❌ No se encontraron semanas")
        return
    
    mostrar_semanas_disponibles(enlaces)
    
    # Procesar semanas
    print("\n" + "="*60)
    print("⏳ EXTRAYENDO DATOS")
    print("="*60 + "\n")
    
    datos_todas = []
    errores = []
    
    for i, semana in enumerate(enlaces, 1):
        try:
            print(f"  [{i:2d}/{len(enlaces)}] {semana['titulo'][:40]}...", end=" ")
            datos = extraer_datos_reunion(semana['url'])
            
            if datos:
                print("✅")
                datos_todas.append(datos)
            else:
                print("❌")
                errores.append(semana['titulo'])
                
        except Exception as e:
            print(f"❌ ({str(e)[:30]})")
            errores.append(f"{semana['titulo']}: {e}")
    
    # Resumen
    print("\n" + "="*60)
    print(f"📊 RESUMEN")
    print("="*60)
    print(f"  ✅ Procesadas exitosamente: {len(datos_todas)}/{len(enlaces)}")
    if errores:
        print(f"  ❌ Con errores: {len(errores)}")
    print("="*60 + "\n")
    
    if not datos_todas:
        print("❌ No hay datos para guardar")
        return
    
    # Guardar en Google Sheets
    gc = conectar_google_sheets()
    
    if not gc:
        print("❌ No se pudo conectar a Google Sheets")
        return
    
    titulo = input("\n📝 ¿Nombre para la hoja? (presiona Enter para 'Reuniones JW'): ").strip()
    if not titulo:
        titulo = "Reuniones JW"
    
    spreadsheet_id = crear_plantilla_sheets(gc, titulo)
    
    if spreadsheet_id:
        rellenar_sheets(gc, spreadsheet_id, datos_todas)
        print("\n🎉 ¡PROCESO COMPLETADO EXITOSAMENTE!")
    else:
        print("\n❌ No se pudo crear la hoja de cálculo")

if __name__ == "__main__":
    main()