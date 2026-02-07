# bot_engine/platforms/base.py
import undetected_chromedriver as uc
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
import os
import time
import random
from typing import Optional

class BaseBot:
    # Proxy configuration from environment (same as cookie_validator)
    PROXY_HOST = os.getenv('PROXY_HOST')
    PROXY_PORT = os.getenv('PROXY_PORT')
    PROXY_USERNAME = os.getenv('PROXY_USERNAME')
    PROXY_PASSWORD = os.getenv('PROXY_PASSWORD')
    
    def __init__(self, account):
        self.account = account
        self.driver = None
    
    def _get_sticky_proxy(self) -> Optional[str]:
        """
        Create sticky proxy URL using account ID.
        Same pattern as cookie_validator to ensure consistent IP across sessions.
        
        Returns:
            Proxy URL string or None if not configured
        """
        if not all([self.PROXY_HOST, self.PROXY_PORT, self.PROXY_USERNAME, self.PROXY_PASSWORD]):
            return None
        
        # Create sticky session using account ID
        sticky_username = f"{self.PROXY_USERNAME}-cookie-{self.account.id}"
        proxy_url = f"http://{sticky_username}:{self.PROXY_PASSWORD}@{self.PROXY_HOST}:{self.PROXY_PORT}"
        
        print(f"  ✓ Using sticky proxy session: cookie-{self.account.id}")
        return proxy_url
    
    def _add_stealth_arguments(self, options):
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-web-security')
        options.add_argument('--disable-features=IsolateOrigins,site-per-process')
        options.add_argument('--window-size=1920,1080')
        options.add_argument('--lang=en-US,en')
    
    def _add_preferences(self, options):
        options.add_experimental_option('prefs', {
            'intl.accept_languages': 'en-US,en',
            'profile.default_content_setting_values.notifications': 2,
        })
    
    def _get_anti_detection_options(self):
        options = uc.ChromeOptions()
        profile_path = self.account.get_profile_path()
        options.add_argument(f"--user-data-dir={profile_path}")
        
        self._add_stealth_arguments(options)
        self._add_preferences(options)
        
        proxy_url = self._get_sticky_proxy()
        if proxy_url:
            options.add_argument(f'--proxy-server={proxy_url}')
        
        return options

    def start_browser(self):
        print("Starting browser with anti-detection measures...")
        options = self._get_anti_detection_options()
        
        self.driver = uc.Chrome(
            options=options,
            version_main=144,
            use_subprocess=True,
        )
        
        self._inject_stealth_scripts()
        print("  ✓ Browser started with stealth mode")
        return self.driver
    
    def _inject_webdriver_override(self):
        self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': 'Object.defineProperty(navigator, "webdriver", {get: () => undefined});'
        })
    
    def _inject_plugins_override(self):
        self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': 'Object.defineProperty(navigator, "plugins", {get: () => [1, 2, 3, 4, 5]});'
        })
    
    def _inject_permissions_override(self):
        self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': '''const originalQuery = window.navigator.permissions.query;
                window.navigator.permissions.query = (parameters) => (
                    parameters.name === 'notifications' ?
                        Promise.resolve({ state: Notification.permission }) :
                        originalQuery(parameters)
                );'''
        })
    
    def _inject_navigator_properties(self):
        self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': '''Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
                Object.defineProperty(navigator, 'platform', {get: () => 'Win32'});
                Object.defineProperty(navigator, 'vendor', {get: () => 'Google Inc.'});'''
        })
    
    def _inject_chrome_properties(self):
        self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': '''window.chrome = {runtime: {}};
                Object.defineProperty(navigator, 'maxTouchPoints', {get: () => 0});'''
        })
    
    def _inject_stealth_scripts(self):
        if not self.driver:
            return
        self._inject_webdriver_override()
        self._inject_plugins_override()
        self._inject_permissions_override()
        self._inject_navigator_properties()
        self._inject_chrome_properties()
    
    def human_delay(self, min_seconds: float = 0.5, max_seconds: float = 2.0):
        time.sleep(random.uniform(min_seconds, max_seconds))
    
    def human_type(self, element, text: str, min_delay: float = 0.05, max_delay: float = 0.15):
        for char in text:
            element.send_keys(char)
            time.sleep(random.uniform(min_delay, max_delay))
    
    def human_click(self, element):
        self.human_delay(0.3, 0.8)
        actions = ActionChains(self.driver)
        offset_x = random.randint(-5, 5)
        offset_y = random.randint(-5, 5)
        actions.move_to_element_with_offset(element, offset_x, offset_y).pause(
            random.uniform(0.1, 0.3)
        ).click().perform()
    
    def random_scroll(self):
        scroll_amount = random.randint(100, 500)
        direction = random.choice([1, -1])
        self.driver.execute_script(f"window.scrollBy(0, {scroll_amount * direction});")
        self.human_delay(0.5, 1.5)
    
    def move_mouse_random(self):
        try:
            actions = ActionChains(self.driver)
            x = random.randint(100, 500)
            y = random.randint(100, 400)
            actions.move_by_offset(x, y).perform()
        except:
            pass

    def close_browser(self, delay=3):
        if self.driver:
            print(f"\nClosing browser in {delay} seconds...")
            time.sleep(delay)
            try:
                self.driver.quit()
                print("Browser closed successfully.")
            except Exception as e:
                print(f"Error closing browser: {str(e)}")

    def save_cookies(self):
        if not self.driver:
            raise Exception("Driver not initialized")
        cookies = self.driver.get_cookies()
        self.account.update_cookies(cookies)
        print(f"Cookies saved to database ({len(cookies)} cookies)")
        return True

    def load_cookies(self):
        if not self.driver:
            raise Exception("Driver not initialized")
        if self.account.cookies:
            for cookie in self.account.cookies:
                try:
                    self.driver.add_cookie(cookie)
                except Exception as e:
                    print(f"  Skipped invalid cookie: {e}")
            print(f"Cookies loaded from database ({len(self.account.cookies)} cookies)")
            return True
        print("No cookies found in database")
        return False

    def wait_for_element(self, by, value, timeout=10):
        return WebDriverWait(self.driver, timeout).until(
            EC.presence_of_element_located((by, value))
        )
    
    def find_element_safe(self, by, value):
        try:
            return self.driver.find_element(by, value)
        except:
            return None

    def is_logged_in(self):
        cookies = self.driver.get_cookies()
        cookie_names = [cookie['name'] for cookie in cookies]
        session_patterns = ['sessionid', 'session', 'auth', 'token', 'sid']
        return any(
            any(pattern in cookie_name.lower() for pattern in session_patterns)
            for cookie_name in cookie_names
        )

    def wait_for_manual_action(self, timeout=300, check_interval=15):
        print(f"Waiting up to {timeout} seconds for manual action (2FA, etc.)...")
        print(f"Checking login status every {check_interval} seconds...\n")
        
        start_time = time.time()
        initial_url = self.driver.current_url
        check_count = 0
        
        while time.time() - start_time < timeout:
            time.sleep(check_interval)
            check_count += 1
            elapsed = int(time.time() - start_time)
            
            print(f"[Check #{check_count} at {elapsed}s] Checking login status...")
            
            current_url = self.driver.current_url
            url_changed = current_url != initial_url
            logged_in = self.is_logged_in()
            
            if url_changed:
                print(f"  ✓ URL changed: {initial_url} → {current_url}")
            
            if logged_in:
                print(f"  ✓ Login verified via platform-specific checks")
                print(f"\n✅ Login successful after {elapsed} seconds!")
                return True
            else:
                print(f"  ⏳ Still waiting for login confirmation...")
        
        print(f"\n⏰ Timeout reached after {timeout} seconds")
        print("Performing final login check...")
        
        if self.is_logged_in():
            print("✅ Login appears successful!")
            return True
        
        print("❌ Could not confirm successful login")
        return False

    def login(self):
        raise NotImplementedError("Subclasses must implement login()")