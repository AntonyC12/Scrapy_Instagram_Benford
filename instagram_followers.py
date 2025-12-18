"""
Instagram Follower Stats Scraper - VERSIÓN HÍBRIDA OPTIMIZADA
Selenium para login + extracción inicial
Playwright paralelo para análisis de perfiles (10x más rápido)
"""

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from playwright.async_api import async_playwright
import asyncio
from time import sleep
import os
import datetime
import random
import csv
import re
import json
from dotenv import load_dotenv, find_dotenv
# --- Imports para análisis de Benford ---
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import math

dotenv_path = find_dotenv()
if not dotenv_path:
    print("❌ ERROR: No se encontró un archivo .env en el directorio del script.")
    print("Copia el archivo .env.example a .env y completa las variables necesarias:")
    print("  IG_USERNAME=tu_usuario")
    print("  IG_PASSWORD=tu_contraseña")
    print("  TARGET_ACCOUNT=cuenta_a_scrapear")
    print("  FOLLOWER_COUNT=50")
    print("  PAGE_TYPE=followers  # o following")
    exit(1)

# Cargar el .env detectado
load_dotenv(dotenv_path)

# ====================== CONFIGURACIÓN ======================
# Cargar variables desde .env (sin valores por defecto "peligrosos")
yourusername = os.getenv("IG_USERNAME", "").strip()
yourpassword = os.getenv("IG_PASSWORD", "").strip()
account = os.getenv("TARGET_ACCOUNT", "").strip()
count_str = os.getenv("FOLLOWER_COUNT", "20").strip()
count = int(count_str)
page = os.getenv("PAGE_TYPE", "followers").strip().lower()

# Configuración de paralelización
MAX_CONCURRENT_WORKERS = 15  # Número de perfiles que se analizarán simultáneamente
# Recomendado: 5-10 (seguro), 15-20 (arriesgado pero rápido)

yourusername = os.getenv("IG_USERNAME")
yourpassword = os.getenv("IG_PASSWORD")

if not yourusername or not yourpassword:
    print("❌ ERROR: Credenciales no configuradas")
    print("Crea un archivo .env con:")
    print("IG_USERNAME=tu_usuario")
    print("IG_PASSWORD=tu_contraseña")
    exit(1)

# ====================== LOGGER ======================
class Logger:
    def __init__(self, log_dir="logs"):
        self.logs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), log_dir)
        if not os.path.exists(self.logs_dir):
            os.makedirs(self.logs_dir)
        
        self.timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        self.log_file = os.path.join(self.logs_dir, f"hybrid_log_{self.timestamp}.txt")
        self.csv_file = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), 
            f"{account}stats_hybrid{self.timestamp}.csv"
        )
        self.txt_file = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), 
            f"{account}stats_hybrid{self.timestamp}.txt"
        )
        self.cookies_file = os.path.join(self.logs_dir, f"cookies_{self.timestamp}.json")
        
    def log(self, message, level="INFO"):
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        formatted_message = f"[{timestamp}] [{level}] {message}"
        print(formatted_message)
        with open(self.log_file, 'a', encoding='utf-8') as f:
            f.write(formatted_message + "\n")
    
    def error(self, message):
        self.log(message, "ERROR")
    
    def warning(self, message):
        self.log(message, "WARNING")
    
    def success(self, message):
        self.log(message, "SUCCESS")
    
    def debug(self, message):
        self.log(message, "DEBUG")

logger = Logger()

# ====================== UTILIDADES ======================
def human_delay(min_seconds=1.0, max_seconds=3.0):
    sleep(random.uniform(min_seconds, max_seconds))

def type_like_human(element, text):
    for char in text:
        element.send_keys(char)
        sleep(random.uniform(0.05, 0.15))

def parse_follower_count(text):
    """
    Extrae el número de seguidores de un texto con máxima precisión
    Ejemplos: 
        "1,234 followers" -> 1234
        "3,223 followers" -> 3223
        "1.2M followers" -> 1200000
        "10.5K followers" -> 10500
        "1333 followers" -> 1333
    """
    if not text:
        return None
    
    text = text.lower().strip()
    
    # Patrones en orden de especificidad
    patterns = [
        (r'([\d,\.]+)\s*m\s*followers?', 'M'),  # Millones
        (r'([\d,\.]+)\s*k\s*followers?', 'K'),  # Miles
        (r'([\d,\.]+)\s*followers?', None),     # Número exacto
    ]
    
    for pattern, unit in patterns:
        match = re.search(pattern, text)
        if match:
            num_str = match.group(1)
            
            if unit == 'M':
                # Para millones: "1.2M" -> 1200000
                num = float(num_str.replace(',', '.'))
                return int(num * 1_000_000)
            
            elif unit == 'K':
                # Para miles: "10.5K" -> 10500
                num = float(num_str.replace(',', '.'))
                return int(num * 1_000)
            
            else:
                # Para números exactos sin K/M
                # Eliminar TODAS las comas y puntos (separadores de miles)
                # "1,234" -> "1234"
                # "3.223" (formato europeo) -> "3223"
                # "1,333" -> "1333"
                clean_num = num_str.replace(',', '').replace('.', '')
                
                # Validar que solo contenga dígitos
                if clean_num.isdigit():
                    return int(clean_num)
                else:
                    # Si no es un número válido, intentar parsearlo de todos modos
                    try:
                        return int(float(clean_num))
                    except:
                        continue
    
    return None

# ====================== SELENIUM: LOGIN Y EXTRACCIÓN DE LISTA ======================
def setup_selenium_driver():
    """Configura driver de Selenium"""
    options = webdriver.ChromeOptions()
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
    options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
    options.add_experimental_option('useAutomationExtension', False)
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.maximize_window()
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    return driver

def handle_cookies(driver):
    """Maneja cookies"""
    cookie_selectors = [
        (By.XPATH, "//button[contains(text(),'Allow essential and optional cookies')]"),
        (By.XPATH, "//button[contains(text(),'Accept')]"),
    ]
    
    for by, selector in cookie_selectors:
        try:
            btn = WebDriverWait(driver, 3).until(EC.element_to_be_clickable((by, selector)))
            btn.click()
            human_delay(1, 2)
            return True
        except:
            continue
    return False

def selenium_login(driver):
    """Login rápido sin validaciones múltiples"""
    try:
        logger.log("🔐 Iniciando login rápido con Selenium...")
        driver.get('https://www.instagram.com/')
        sleep(3)  # Espera corta: solo para carga inicial y cookies

        # Aceptar cookies si aparecen
        handle_cookies(driver)

        # Llenar usuario y contraseña
        username_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.NAME, "username"))
        )
        password_input = driver.find_element(By.NAME, "password")

        username_input.clear()
        password_input.clear()

        type_like_human(username_input, yourusername)
        type_like_human(password_input, yourpassword)
        sleep(0.5)

        # Enviar formulario directamente sin validación posterior
        password_input.send_keys(Keys.ENTER)
        logger.log("🚀 Enviando credenciales (modo rápido)...")

        # Espera breve y pasar directo a la cuenta objetivo
        sleep(random.uniform(4, 6))

        # Abrir directamente la cuenta a scrapear
        target_url = f"https://www.instagram.com/{account}/"
        driver.get(target_url)
        logger.success(f"✓ Login rápido completado. Abriendo cuenta: {account}")

        # Espera un poco para cargar correctamente
        sleep(random.uniform(1.5, 2.5))
        return True

    except Exception as e:
        logger.error(f"❌ Error en login rápido: {str(e)}")
        return False


def handle_post_login_dialogs(driver):
    """Cerrar diálogos post-login"""
    dialog_buttons = [
        (By.XPATH, "//button[contains(text(),'Not Now')]"),
        (By.XPATH, "//button[contains(text(),'Ahora no')]"),
    ]
    
    for _ in range(2):
        human_delay(2, 3)
        for by, selector in dialog_buttons:
            try:
                btn = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((by, selector)))
                btn.click()
                logger.debug("Diálogo cerrado")
                break
            except:
                continue

def scroll_modal_smart(driver):
    """
    Hace scroll inteligente buscando el div correcto que scrollea
    Basado en técnica probada que busca divs con scrollHeight > clientHeight
    """
    try:
        # JavaScript que busca automáticamente el div scrolleable correcto
        scroll_script = """
        const dialog = document.querySelector('div[role="dialog"]');
        if (!dialog) return false;
        
        // Buscar el div que realmente scrollea
        const divs = dialog.querySelectorAll('div');
        for (let div of divs) {
            // Si el div tiene contenido scrolleable (20% más alto que visible)
            if (div.scrollHeight > div.clientHeight * 1.2) {
                div.scrollTop = div.scrollHeight;
                return true;
            }
        }
        return false;
        """
        
        result = driver.execute_script(scroll_script)
        
        if result:
            # Pausa para que Instagram cargue más datos
            sleep(random.uniform(1.5, 2.5))
            return True
        else:
            logger.debug("  ⚠ No se encontró div scrolleable")
            return False
        
    except Exception as e:
        logger.debug(f"  ✗ Error en scroll: {str(e)}")
        return False

def extract_followers_list_selenium(driver, account_name, page_type, target_count):
    """Extrae lista de seguidores con Selenium usando clic tradicional y scroll automático"""
    try:
        logger.log(f"📋 Extrayendo lista de {page_type} de {account_name}...")
        logger.log(f"🎯 Objetivo: {target_count} usuarios")

        # Ya estamos en la cuenta, solo pequeña pausa para carga del perfil
        human_delay(1.5, 2.2)

        # Verificar si la cuenta existe
        try:
            driver.find_element(By.XPATH, "//h2[contains(text(), 'Sorry')]")
            logger.error("❌ Cuenta no existe o no accesible")
            return []
        except NoSuchElementException:
            logger.debug("✓ Cuenta accesible")

        # --- 🔹 ABRIR MODAL DE FOLLOWERS (clic tradicional, no carga directa) ---
        logger.log(f"🖱️ Abriendo modal de {page_type} con clic tradicional...")
        try:
            followers_link = WebDriverWait(driver, 6).until(
                EC.element_to_be_clickable((By.XPATH, f'//a[contains(@href, "/{page_type}")]'))
            )
            driver.execute_script("arguments[0].click();", followers_link)
        except TimeoutException:
            logger.error(f"❌ No se encontró el enlace de {page_type}")
            return []

        # --- Espera corta para el modal (sin detección agresiva) ---
        try:
            modal = WebDriverWait(driver, 7).until(
                EC.presence_of_element_located((By.XPATH, "//div[@role='dialog']"))
            )
            logger.success(f"✓ Modal de {page_type} abierto correctamente.")
        except TimeoutException:
            logger.error("❌ No se detectó el modal después del clic.")
            return []

        human_delay(1.5, 2.5)
        logger.log("⏳ Cargando primeros usuarios visibles...")

        # --- Extracción y scroll automático (idéntico, no tocar) ---
        followers_list = []
        scraped = set()
        consecutive_no_progress = 0
        max_no_progress = 10
        scroll_attempts = 0
        max_scroll_attempts = 200

        logger.log("🔄 Iniciando extracción con scroll inteligente...")
        logger.log(f"   Objetivo: {target_count}")
        logger.log(f"   Máx intentos sin progreso: {max_no_progress}")

        while len(followers_list) < target_count and consecutive_no_progress < max_no_progress and scroll_attempts < max_scroll_attempts:
            user_links = driver.find_elements(By.XPATH, "//div[@role='dialog']//a[contains(@href, '/')]")
            new_users = 0

            for link in user_links:
                try:
                    href = link.get_attribute('href')
                    if href and 'instagram.com/' in href:
                        username = href.split('instagram.com/')[-1].strip('/').split('/')[0]
                        if username and username not in scraped and username != account_name and not username.startswith(('explore', 'p/', 'direct')):
                            scraped.add(username)
                            followers_list.append(username)
                            new_users += 1
                            if len(followers_list) >= target_count:
                                logger.success(f"🎯 Objetivo alcanzado ({len(followers_list)})")
                                break
                except Exception:
                    continue

            # Control de progreso
            if new_users > 0:
                consecutive_no_progress = 0
                logger.log(f"  ✓ Progreso: {len(followers_list)}/{target_count} (+{new_users})")
            else:
                consecutive_no_progress += 1
                if consecutive_no_progress <= 3:
                    logger.debug(f"  ⏳ Sin nuevos usuarios ({consecutive_no_progress})")
                else:
                    logger.warning(f"  ⚠ Sin progreso ({consecutive_no_progress})")

            if len(followers_list) >= target_count:
                break

            scroll_attempts += 1
            if scroll_attempts % 10 == 1:
                logger.debug(f"  📜 Scroll #{scroll_attempts}")

            # Mantiene scroll automático existente (sin tocar)
            scroll_modal_smart(driver)

            # Pausa corta entre scrolls (natural)
            sleep(random.uniform(1.4, 2.2))

        # --- Resumen final ---
        logger.log("=" * 60)
        if len(followers_list) >= target_count:
            logger.success(f"✅ ÉXITO: {len(followers_list)} usuarios extraídos")
        elif followers_list:
            logger.warning(f"⚠ PARCIAL: {len(followers_list)}/{target_count} usuarios (fin de lista o límite de carga)")
        else:
            logger.error("❌ No se extrajo ningún usuario")

        logger.log(f"   Total scrolls: {scroll_attempts}")
        logger.log("=" * 60)
        return followers_list

    except Exception as e:
        logger.error(f"❌ Error extrayendo lista: {str(e)}")
        import traceback
        logger.debug(traceback.format_exc())
        return []

def save_selenium_cookies(driver, filepath):
    """Guarda cookies de Selenium para reutilizarlas en Playwright"""
    try:
        cookies = driver.get_cookies()
        with open(filepath, 'w') as f:
            json.dump(cookies, f)
        logger.success(f"✓ Cookies guardadas: {filepath}")
        return True
    except Exception as e:
        logger.error(f"Error guardando cookies: {str(e)}")
        return False

# ====================== PLAYWRIGHT: ANÁLISIS PARALELO ======================
async def get_follower_count_playwright(context, username, worker_id):
    """
    Obtiene el número de seguidores de un usuario usando Playwright
    """
    page = None
    try:
        page = await context.new_page()
        
        # Bloquear recursos innecesarios para mayor velocidad
        await page.route("/*.{png,jpg,jpeg,gif,svg,mp4,webm}", lambda route: route.abort())
        await page.route("/static/", lambda route: route.abort())
        
        url = f'https://www.instagram.com/{username}/'
        await page.goto(url, wait_until='domcontentloaded', timeout=15000)
        
        # Esperar un poco para que cargue
        await page.wait_for_timeout(2000)
        
        # Verificar si existe
        try:
            error = await page.query_selector("h2:has-text('Sorry')")
            if error:
                logger.warning(f"  [Worker {worker_id}] ⚠ {username} no existe/privado")
                return username, None
        except:
            pass
        
        # Buscar número de seguidores
        selectors = [
            f'a[href="/{username}/followers/"]',
            'a[href*="/followers/"]',
        ]
        
        for selector in selectors:
            try:
                element = await page.wait_for_selector(selector, timeout=5000)
                if element:
                    text = await element.inner_text()
                    count = parse_follower_count(text)
                    
                    if count is not None:
                        logger.success(f"  [Worker {worker_id}] ✓ {username}: {count:,}")
                        return username, count
                    
                    # Intentar con title
                    title = await element.get_attribute('title')
                    if title:
                        count = parse_follower_count(title)
                        if count is not None:
                            logger.success(f"  [Worker {worker_id}] ✓ {username}: {count:,}")
                            return username, count
            except:
                continue
        
        # Método alternativo: buscar en todo el texto
        try:
            body_text = await page.inner_text('body')
            if 'followers' in body_text.lower():
                lines = body_text.split('\n')
                for line in lines:
                    if 'follower' in line.lower():
                        count = parse_follower_count(line)
                        if count is not None:
                            logger.success(f"  [Worker {worker_id}] ✓ {username}: {count:,} (alt)")
                            return username, count
        except:
            pass
        
        logger.warning(f"  [Worker {worker_id}] ⚠ No se pudo obtener de {username}")
        return username, None
        
    except Exception as e:
        logger.debug(f"  [Worker {worker_id}] ✗ Error en {username}: {str(e)}")
        return username, None
    finally:
        if page:
            await page.close()

# -------------------------
# Nuevo: obtener info de perfil (para PAGE_TYPE == 'following')
# -------------------------
async def get_profile_info_playwright(context, username, worker_id):
    """
    Extrae: name, username, bio (description), account_type (categoria), num_followers (si está).
    Devuelve: (username, details_dict) donde details_dict = {
        'name': str|None,
        'username': username,
        'bio': str|None,
        'account_type': str|None,
        'num_followers': int|None
    }
    """
    page = None
    try:
        page = await context.new_page()
        # Bloquear recursos pesados
        await page.route("/*.{png,jpg,jpeg,gif,svg,mp4,webm}", lambda route: route.abort())
        await page.route("/static/", lambda route: route.abort())

        url = f'https://www.instagram.com/{username}/'
        await page.goto(url, wait_until='domcontentloaded', timeout=15000)
        await page.wait_for_timeout(1500)

        # Si la cuenta no existe o es privada detectada por texto tipo 'Sorry'
        try:
            err = await page.query_selector("h2:has-text('Sorry')")
            if err:
                logger.warning(f"  [Worker {worker_id}] ⚠ {username} no existe/privado")
                return username, {
                    'name': None,
                    'username': username,
                    'bio': None,
                    'account_type': None,
                    'num_followers': None
                }
        except:
            pass

        async def try_selectors_text(selectors):
            for s in selectors:
                try:
                    el = await page.query_selector(s)
                    if el:
                        txt = (await el.inner_text()).strip()
                        if txt:
                            return txt
                except:
                    continue
            return None

        # Intentar extraer name (varios selectores por si cambia el DOM)
        name_selectors = [
            "header h1", "header section h1", "main header h1", "h1"
        ]
        name = await try_selectors_text(name_selectors)

        # Intentar extraer bio/description (multi-fallback)
        bio_selectors = [
            "div.-vDIg > span",                 # antiguo
            "section div:nth-of-type(2) span",  # fallback
            "div[data-testid='user-bio']",      # posible selector semántico
            "main section div span",            # genérico
            "header + div span"                 # alternativa
        ]
        bio = await try_selectors_text(bio_selectors)

        # Intentar extraer account_type / category (empresas, noticias, artista...)
        acct_selectors = [
            "header section div a[role='link']",    # a veces es link
            "header section div span",              # fallback
            "div._aa_c span"                        # fallback genérico
        ]
        account_type = await try_selectors_text(acct_selectors)

        # Como fallback adicional, intentar leer meta description (puede contener texto util)
        if not bio or not name or not account_type:
            try:
                meta_desc = await page.locator('meta[name="description"]').get_attribute('content')
                if meta_desc:
                    meta_desc = meta_desc.strip()
                    # meta suele contener: "Nombre (@username) • X posts • Y followers • Z following"
                    # o bien la bio en algunos casos; usar sólo si falta bio
                    if not bio:
                        # Si meta tiene guiones o "•", nos quedamos con la parte antes de '•' si parece texto libre
                        parts = [p.strip() for p in re.split(r'•|-', meta_desc) if p.strip()]
                        # Eliminar segmento que contenga 'followers' o 'posts' (es meta técnica)
                        candidate = None
                        for p in parts:
                            if 'followers' not in p.lower() and 'posts' not in p.lower() and '@' not in p:
                                candidate = p
                                break
                        if candidate:
                            bio = candidate

                    # Si name está ausente, intentar extraer antes del '('
                    if not name:
                        m = re.match(r"^(.*?)\s*\(", meta_desc)
                        if m:
                            name_c = m.group(1).strip()
                            if name_c:
                                name = name_c
            except:
                pass

        # Intentar extraer número de followers usando las mismas técnicas que ya tienes
        followers_count = None
        try:
            # Reusar la búsqueda por selectores que ya conocemos
            for sel in [f'a[href="/{username}/followers/"]', 'a[href*="/followers/"]']:
                try:
                    elt = await page.query_selector(sel)
                    if elt:
                        text = (await elt.inner_text()).strip()
                        cnt = parse_follower_count(text)
                        if cnt is not None:
                            followers_count = cnt
                            break
                        title = await elt.get_attribute('title')
                        if title:
                            cnt = parse_follower_count(title)
                            if cnt is not None:
                                followers_count = cnt
                                break
                except:
                    continue

            # fallback: buscar en todo el texto del body
            if followers_count is None:
                body_text = await page.inner_text('body')
                if 'followers' in body_text.lower():
                    for line in body_text.split('\n'):
                        if 'follower' in line.lower():
                            cnt = parse_follower_count(line)
                            if cnt is not None:
                                followers_count = cnt
                                break
        except:
            pass

        details = {
            'name': name,
            'username': username,
            'bio': bio,
            'account_type': account_type,
            'num_followers': followers_count
        }

        logger.success(f"  [Worker {worker_id}] ✓ {username} info: name={'OK' if name else 'N/A'}, bio={'OK' if bio else 'N/A'}, type={'OK' if account_type else 'N/A'}, followers={followers_count if followers_count is not None else 'N/A'}")
        return username, details

    except Exception as e:
        logger.debug(f"  [Worker {worker_id}] ✗ Error en profile {username}: {str(e)}")
        return username, {
            'name': None,
            'username': username,
            'bio': None,
            'account_type': None,
            'num_followers': None
        }
    finally:
        if page:
            await page.close()

async def process_batch(context, batch, worker_id, semaphore):
    """Procesa un lote de usuarios con un worker. Si page == 'following' extrae perfil completo."""
    async with semaphore:
        results = []
        for username in batch:
            if page == 'following':
                result = await get_profile_info_playwright(context, username, worker_id)
            else:
                result = await get_follower_count_playwright(context, username, worker_id)
            results.append(result)
            # Pequeña pausa entre perfiles del mismo worker
            await asyncio.sleep(random.uniform(0.5, 1.5))
        return results
    """Procesa un lote de usuarios con un worker"""
    async with semaphore:
        results = []
        for username in batch:
            result = await get_follower_count_playwright(context, username, worker_id)
            results.append(result)
            # Pequeña pausa entre perfiles del mismo worker
            await asyncio.sleep(random.uniform(0.5, 1.5))
        return results

async def analyze_profiles_parallel(cookies_file, followers_list, max_workers):
    """
    Analiza perfiles en paralelo usando Playwright
    """
    logger.log("="*80)
    logger.log(f"🚀 INICIANDO ANÁLISIS PARALELO CON {max_workers} WORKERS")
    logger.log("="*80)
    
    # Cargar cookies
    with open(cookies_file, 'r') as f:
        selenium_cookies = json.load(f)
    
    # Dividir la lista en lotes para cada worker
    batch_size = len(followers_list) // max_workers
    if batch_size == 0:
        batch_size = 1
    
    batches = [followers_list[i:i+batch_size] for i in range(0, len(followers_list), batch_size)]
    
    logger.log(f"📦 {len(followers_list)} usuarios divididos en {len(batches)} lotes")
    
    results = []
    
    async with async_playwright() as p:
        # Lanzar navegador
        browser = await p.chromium.launch(
            headless=True,  # Cambiar a False para ver el proceso
            args=['--disable-blink-features=AutomationControlled']
        )
        
        # Crear contexto con cookies
        context = await browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            viewport={'width': 1920, 'height': 1080}
        )
        
        # Añadir cookies de Selenium a Playwright
        playwright_cookies = []
        for cookie in selenium_cookies:
            playwright_cookie = {
                'name': cookie['name'],
                'value': cookie['value'],
                'domain': cookie['domain'],
                'path': cookie['path'],
            }
            if 'expiry' in cookie:
                playwright_cookie['expires'] = cookie['expiry']
            if 'secure' in cookie:
                playwright_cookie['secure'] = cookie['secure']
            if 'httpOnly' in cookie:
                playwright_cookie['httpOnly'] = cookie['httpOnly']
            
            playwright_cookies.append(playwright_cookie)
        
        await context.add_cookies(playwright_cookies)
        logger.success("✓ Cookies cargadas en Playwright")
        
        # Semáforo para limitar concurrencia
        semaphore = asyncio.Semaphore(max_workers)
        
        # Crear tareas para cada lote
        tasks = []
        for worker_id, batch in enumerate(batches, 1):
            task = process_batch(context, batch, worker_id, semaphore)
            tasks.append(task)
        
        # Ejecutar todas las tareas en paralelo
        logger.log(f"⏱  Tiempo estimado: ~{len(followers_list) * 2 / max_workers / 60:.1f} minutos")
        start_time = datetime.datetime.now()
        
        batch_results = await asyncio.gather(*tasks)
        
        end_time = datetime.datetime.now()
        elapsed = (end_time - start_time).total_seconds()
        
        # Consolidar resultados
        for batch_result in batch_results:
            results.extend(batch_result)
        
        await browser.close()
        
        logger.log("="*80)
        logger.success(f"✅ ANÁLISIS PARALELO COMPLETADO")
        logger.log(f"⏱  Tiempo real: {elapsed/60:.1f} minutos")
        logger.log(f"🚀 Velocidad: {len(results)/(elapsed/60):.1f} perfiles/minuto")
        logger.log("="*80)
    
    return results

# Análisis de Benford
def benford_analysis(csv_path, save_fig=True, show_plot=True):
    """
    Versión mejorada visualmente de Benford:
    - Barras azules (Porcentaje real)
    - Porcentajes en color negro
    - Tabla debajo del gráfico
    - Líneas comparativas con la Ley de Benford
    """
    try:
        df = pd.read_csv(csv_path, dtype=str)
    except Exception as e:
        logger.error(f"❌ No se pudo leer CSV para Benford: {e}")
        return

    # Normalizar nombres de columnas (soportar español/inglés)
    col_first_digit = None
    if 'Primer_Dígito' in df.columns:
        col_first_digit = 'Primer_Dígito'
    elif 'First_Digit' in df.columns:
        col_first_digit = 'First_Digit'
    elif 'Primer_Digito' in df.columns:
        col_first_digit = 'Primer_Digito'
    elif 'Primer Digito' in df.columns:
        col_first_digit = 'Primer Digito'

    if col_first_digit is None:
        num_col = None
        if 'Num_Followers' in df.columns:
            num_col = 'Num_Followers'
        elif 'NumFollowers' in df.columns:
            num_col = 'NumFollowers'
        elif 'Num Seguidores' in df.columns:
            num_col = 'Num Seguidores'
        if num_col is None:
            logger.error("❌ CSV no contiene 'Primer_Dígito' ni 'Num_Followers'. No se puede aplicar Benford.")
            return

        def first_digit_from_value(v):
            try:
                s = str(v).strip()
                s = re.sub(r'[^0-9]', '', s)
                if not s:
                    return None
                s = s.lstrip('0')
                return int(s[0]) if s else None
            except:
                return None

        df['__first_digit__'] = df[num_col].apply(first_digit_from_value)
        digits_series = df['__first_digit__'].dropna().astype(int).tolist()
    else:
        def normalize_digit(x):
            try:
                if pd.isna(x):
                    return None
                sx = str(x).strip()
                sx = re.sub(r'[^0-9]', '', sx)
                if sx == '':
                    return None
                d = int(sx[0])
                if 1 <= d <= 9:
                    return d
                return None
            except:
                return None

        digits_series = df[col_first_digit].apply(normalize_digit).dropna().astype(int).tolist()

    if not digits_series:
        logger.error("❌ No se encontraron primeros dígitos válidos para analizar.")
        return

    # Calcular frecuencias y porcentajes
    total = len(digits_series)
    frecuencias_reales = [digits_series.count(d) for d in range(1, 10)]
    porcentajes_reales = [(f / total) * 100 for f in frecuencias_reales]
    porcentajes_benford = [(math.log10(1 + 1/d)) * 100 for d in range(1, 10)]
    digitos = np.arange(1, 10)

    # --- Layout con gridspec: gráfico arriba, tabla abajo ---
    fig = plt.figure(figsize=(12, 9))
    gs = fig.add_gridspec(3, 1, height_ratios=[3, 0.05, 1], hspace=0.35)
    ax = fig.add_subplot(gs[0, 0])
    ax_table_holder = fig.add_subplot(gs[2, 0])
    ax_table_holder.axis('off')

    # --- Barras (Porcentaje real) ---
    bar_width = 0.6
    bars = ax.bar(digitos, porcentajes_reales, width=bar_width, alpha=0.9,
                  label="Porcentaje real (%)", color='steelblue',
                  edgecolor='black', linewidth=0.6)

    # --- Línea de Benford ---
    ax.plot(digitos, porcentajes_benford, marker='o', linestyle='-', linewidth=2.2,
            label="Ley de Benford (%)", color='crimson')

    # --- Anotaciones (porcentajes en negro) ---
    for i, v in enumerate(porcentajes_reales):
        x = digitos[i]
        if v >= 7:
            y_text = v - 1.0
            va = 'top'
        else:
            y_text = v + 0.7
            va = 'bottom'
        ax.text(x, y_text, f"{v:.1f}%", ha='center', va=va,
                fontsize=11, color='black', fontweight='bold')

    # --- Estética del gráfico ---
    ax.set_title("Ley de Benford aplicada a número de seguidores",
                 fontsize=20, fontweight='bold', pad=14)
    ax.set_xlabel("Primer dígito", fontsize=14)
    ax.set_ylabel("Porcentaje (%)", fontsize=14)
    ax.set_xticks(digitos)
    ax.set_xticklabels([str(int(d)) for d in digitos], fontsize=13)
    ax.tick_params(axis='y', labelsize=12)
    ax.grid(axis='y', linestyle='--', alpha=0.45)
    ax.set_ylim(0, max(max(porcentajes_reales) + 6, max(porcentajes_benford) + 6))
    ax.legend(fontsize=12, loc='upper right')

    # --- Tabla debajo del gráfico ---
    tabla_data = []
    for i in range(9):
        tabla_data.append([
            int(digitos[i]),
            frecuencias_reales[i],
            f"{porcentajes_reales[i]:.2f}%",
            f"{porcentajes_benford[i]:.2f}%"
        ])
    tabla_data.append([
        "Total",
        sum(frecuencias_reales),
        f"{sum(porcentajes_reales):.2f}%",
        f"{sum(porcentajes_benford):.2f}%"
    ])

    column_labels = ["Dígito", "Frecuencia", "% Real", "% Benford"]

    table = ax_table_holder.table(cellText=tabla_data,
                                  colLabels=column_labels,
                                  cellLoc='center',
                                  loc='center',
                                  colLoc='center')
    table.auto_set_font_size(False)
    table.set_fontsize(12)
    table.scale(1, 1.2)

    for (row, col), cell in table.get_celld().items():
        cell.set_edgecolor('black')
        cell.set_linewidth(0.6)
        if row == 0:
            cell.set_text_props(fontweight='bold', fontsize=13)
            cell.set_facecolor('#d9e6f2')
        else:
            cell.set_text_props(fontsize=12)
        cell._loc = 'center'

    plt.tight_layout(rect=[0, 0, 1, 0.98])

    if save_fig:
        try:
            logs_dir = os.path.dirname(csv_path) or "."
            fig_name = f"benford_{os.path.splitext(os.path.basename(csv_path))[0]}_{datetime.datetime.now().strftime('%Y%m%d-%H%M%S')}.png"
            fig_path = os.path.join(logs_dir, fig_name)
            plt.savefig(fig_path, dpi=200, bbox_inches='tight')
            logger.success(f"📈 Gráfico Benford guardado: {fig_path}")
        except Exception as e:
            logger.warning(f"⚠ No se pudo guardar figura: {e}")

    if show_plot:
        try:
            plt.show()
        except Exception as e:
            logger.warning(f"⚠ No se pudo mostrar la figura (entorno posiblemente headless): {e}")

    plt.close(fig)

# ====================== GUARDAR RESULTADOS ======================
def save_results(account_name, results_dict):
    """
    Guarda resultados detectando formato:
     - valores int/None -> comportamiento original (Username, Username_Follower, Num_Followers, Primer_Dígito)
     - valores dict -> nuevo formato con: Account, Username, Name, Bio, Account_Type, Num_Followers, Primer_Dígito
    """
    # Detectar si results_dict values son dict o ints
    sample_val = next(iter(results_dict.values()), None)
    writing_extended = isinstance(sample_val, dict)

    # Preparar rows
    if writing_extended:
        # Preparar lista de filas con campos completos
        rows = []
        for username, info in results_dict.items():
            # info es un dict
            name = info.get('name') if isinstance(info, dict) else None
            bio = info.get('bio') if isinstance(info, dict) else None
            account_type = info.get('account_type') if isinstance(info, dict) else None
            num_followers = info.get('num_followers') if isinstance(info, dict) else None
            primer = str(num_followers)[0] if num_followers not in (None, 'None') and str(num_followers).isdigit() else "N/A"
            rows.append([account_name, username, name or "", bio or "", account_type or "", num_followers if num_followers is not None else "", primer])
        # CSV header extended
        header = ['Account', 'Username', 'Name', 'Bio', 'Account_Type', 'Num_Followers', 'Primer_Dígito']

    else:
        # Formato original: username -> int or None
        rows = []
        for username, num_followers in results_dict.items():
            primer = str(num_followers)[0] if num_followers not in (None, 'None') and str(num_followers).isdigit() else "N/A"
            rows.append([username, account_name, num_followers if num_followers is not None else "", primer])
        header = ['Username', 'Username_Follower', 'Num_Followers', 'Primer_Dígito']

    # Escribir CSV
    try:
        with open(logger.csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(rows)
        logger.success(f"📊 CSV generado correctamente: {logger.csv_file}")
    except Exception as e:
        logger.error(f"Error al guardar CSV: {str(e)}")
        return

    # Mantener TXT antiguo sin cambios (opcional: podrías añadir info_extended ahí si quieres)
    try:
        with open(logger.txt_file, 'w', encoding='utf-8') as f:
            f.write(f"{'='*80}\n")
            f.write(f"ANÁLISIS DE SEGUIDORES (HÍBRIDO) - {account_name}\n")
            f.write(f"Fecha: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"{'='*80}\n\n")
            if writing_extended:
                f.write("Formato extendido (seguido -> perfil):\n")
                for account_name_, username, name, bio, account_type, num_followers, primer in rows:
                    f.write(f"{username: <25} | {name: <20} | {account_type or '':<15} | followers: {num_followers or 'N/A'} | 1er: {primer}\n")
            else:
                f.write(f"{'Cuenta':<20} | {'Follower':<25} | {'Num Seguidores':>15} | {'1er Dígito':>10}\n")
                f.write(f"{'-'*20}-+-{'-'*25}-+-{'-'*15}-+-{'-'*10}\n")
                for username, account_name_, num_followers, primer in rows:
                    num_str = f"{num_followers:,}" if num_followers not in (None, '') else "N/A"
                    f.write(f"{account_name_:<20} | {username:<25} | {num_str:>15} | {primer:>10}\n")

        logger.success(f"📄 TXT generado correctamente: {logger.txt_file}")
    except Exception as e:
        logger.error(f"Error al guardar TXT: {str(e)}")

    # --- Ahora que el CSV está escrito, ejecutar Benford (si aplica) ---
    try:
        if os.path.exists(logger.csv_file):
            logger.log("🔎 Ejecutando análisis de Benford sobre el CSV generado...")
            benford_analysis(logger.csv_file, save_fig=True, show_plot=True)
        else:
            logger.error(f"❌ CSV no encontrado para Benford: {logger.csv_file}")
    except Exception as e:
        logger.error(f"❌ Error ejecutando Benford: {e}")

# ====================== MAIN ======================
def main():
    driver = None
    
    try:
        start_time = datetime.datetime.now()
        
        logger.log("="*80)
        logger.log("🎯 SCRAPER HÍBRIDO: SELENIUM + PLAYWRIGHT PARALELO")
        logger.log("="*80)
        logger.log(f"📊 Configuración:")
        logger.log(f"   - Cuenta objetivo: {account}")
        logger.log(f"   - Tipo: {page}")
        logger.log(f"   - Cantidad: {count}")
        logger.log(f"   - Workers paralelos: {MAX_CONCURRENT_WORKERS}")
        logger.log("="*80)
        
        # FASE 1: SELENIUM - Login y extracción de lista
        logger.log("\n" + "="*80)
        logger.log("FASE 1: SELENIUM - LOGIN Y EXTRACCIÓN DE LISTA")
        logger.log("="*80)
        
        driver = setup_selenium_driver()
        logger.success("✓ Driver Selenium iniciado")
        
        if not selenium_login(driver):
            logger.error("❌ Login fallido")
            return
        
        handle_post_login_dialogs(driver)
        
        followers_list = extract_followers_list_selenium(driver, account, page, count)
        
        if not followers_list:
            logger.error("❌ No se pudieron extraer seguidores")
            return
        
        # Guardar cookies para Playwright
        if not save_selenium_cookies(driver, logger.cookies_file):
            logger.error("❌ No se pudieron guardar cookies")
            return
        
        logger.success(f"✓ FASE 1 COMPLETADA: {len(followers_list)} usuarios extraídos")
        
        # Cerrar Selenium
        driver.quit()
        logger.log("✓ Driver Selenium cerrado")
        
        # FASE 2: PLAYWRIGHT - Análisis paralelo
        logger.log("\n" + "="*80)
        logger.log("FASE 2: PLAYWRIGHT - ANÁLISIS PARALELO DE PERFILES")
        logger.log("="*80)
        
        # Ejecutar análisis paralelo
        results = asyncio.run(
            analyze_profiles_parallel(logger.cookies_file, followers_list, MAX_CONCURRENT_WORKERS)
        )
        
        # Convertir resultados a diccionario
        results_dict = {username: count for username, count in results}
        
        # FASE 3: Guardar resultados
        logger.log("\n" + "="*80)
        logger.log("FASE 3: GUARDANDO RESULTADOS")
        logger.log("="*80)
        
        save_results(account, results_dict)
        
        # RESUMEN FINAL
        end_time = datetime.datetime.now()
        total_elapsed = (end_time - start_time).total_seconds()
        
        successful = sum(1 for count in results_dict.values() if count is not None)
        failed = len(results_dict) - successful
        
        logger.log("\n" + "="*80)
        logger.success("🎉 PROCESO COMPLETADO")
        logger.log("="*80)
        logger.log(f"⏱  Tiempo total: {total_elapsed/60:.1f} minutos")
        logger.log(f"🚀 Velocidad promedio: {len(results_dict)/(total_elapsed/60):.1f} perfiles/min")
        logger.log(f"📊 Estadísticas:")
        logger.log(f"   - Total analizado: {len(results_dict)}")
        logger.log(f"   - ✓ Exitosos: {successful}")
        logger.log(f"   - ✗ Fallidos: {failed}")
        logger.log(f"   - Tasa de éxito: {successful/len(results_dict)*100:.1f}%")
        logger.log(f"📁 Archivos generados:")
        logger.log(f"   - CSV: {logger.csv_file}")
        logger.log(f"   - TXT: {logger.txt_file}")
        logger.log(f"   - LOG: {logger.log_file}")
        logger.log("="*80)
        
        # Estimación para 500 perfiles
        if count < 500 and count > 0:
            estimated_time = (total_elapsed / count) * 500 / 60
            logger.log(f"\n💡 Estimación para 500 perfiles: ~{estimated_time:.1f} minutos")
        
    except KeyboardInterrupt:
        logger.warning("\n⚠ Proceso interrumpido por el usuario")
    except Exception as e:
        logger.error(f"\n❌ Error crítico: {str(e)}")
        import traceback
        logger.error(f"Traceback:\n{traceback.format_exc()}")
    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass

if __name__ == "__main__":
    main()
