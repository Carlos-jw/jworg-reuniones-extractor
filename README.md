# 🕊️ Extractor de Reuniones JW.org

Este proyecto automatiza la extracción de información semanal de las **reuniones de los Testigos de Jehová** publicadas en [jw.org](https://www.jw.org).  
El script descarga los datos de cada semana, identifica las partes del programa y los exporta a **Google Sheets** o **Excel**.

---

## 🚀 Características principales

✅ Extrae automáticamente:
- Fecha de la reunión  
- Lectura bíblica semanal  
- Canciones de inicio, intermedia y final  
- Palabras de introducción y conclusión  
- Todas las partes clasificadas por secciones:
  - Tesoros de la Biblia  
  - Seamos mejores maestros  
  - Nuestra vida cristiana  

✅ Permite exportar los datos a:
- 📊 **Google Sheets** (con autenticación)
- 💾 **Archivo Excel local (.xlsx)**

✅ Incluye opción para crear una **plantilla vacía** si deseas rellenarla manualmente.

---

## 🧰 Requisitos

Instala las dependencias necesarias antes de ejecutar el script:

```bash
pip install requests beautifulsoup4 pandas openpyxl gspread google-auth
