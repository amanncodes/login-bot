from .base import BaseBot
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
import time


class InstagramBot(BaseBot):

    def _has_session_cookie(self):
        cookies = self.driver.get_cookies()
        has_sessionid = any(cookie["name"] == "sessionid" for cookie in cookies)
        if has_sessionid:
            print("    ✓ Session cookie detected")
        return has_sessionid
    
    def _is_on_login_page(self):
        username_field = self.find_element_safe(By.CSS_SELECTOR, 'input[name="username"]')
        email_field = self.find_element_safe(By.CSS_SELECTOR, 'input[name="email"]')
        password_field = self.find_element_safe(By.CSS_SELECTOR, 'input[name="password"]') or self.find_element_safe(By.CSS_SELECTOR, 'input[name="pass"]')
        
        if username_field or email_field or password_field:
            print("    ✗ Still on login page")
            return True
        return False
    
    def _has_logged_in_elements(self):
        indicators = [
            (By.XPATH, '//img[contains(@alt, "profile picture")]'),
            (By.CSS_SELECTOR, 'a[href*="/"][role="link"] img[alt*="profile"]'),
            (By.CSS_SELECTOR, 'span[role="link"] img[crossorigin="anonymous"]'),
        ]
        for by, selector in indicators:
            if self.find_element_safe(by, selector):
                print("    ✓ Profile element detected (logged in)")
                return True
        return False
    
    def _is_on_logged_in_page(self):
        current_url = self.driver.current_url
        patterns = ["/direct/", "/explore/", "/reels/"]
        if any(pattern in current_url for pattern in patterns):
            print("    ✓ On logged-in page")
            return True
        return False
    
    def is_logged_in(self):
        try:
            if self._is_on_login_page():
                return False
            if self._has_logged_in_elements():
                return True
            if self._is_on_logged_in_page():
                return True
            if self._has_session_cookie():
                return True
            return False
        except Exception as e:
            print(f"    ! Error checking login status: {str(e)}")
            return False

    def _handle_cookie_consent(self):
        cookie_buttons = [
            '//button[contains(text(), "Accept")]',
            '//button[contains(text(), "Allow")]',
            '//button[text()="Only allow essential cookies"]',
        ]
        for xpath in cookie_buttons:
            btn = self.find_element_safe(By.XPATH, xpath)
            if btn:
                self.human_click(btn)
                print("  ✓ Handled cookie consent")
                self.human_delay(0.5, 1.2)
                break
    
    def _handle_save_info_button(self):
        save_buttons = [
            '//button[contains(text(), "Save Info")]',
            '//button[contains(text(), "Save")]',
            '//button[text()="Save Info"]',
        ]
        for xpath in save_buttons:
            btn = self.find_element_safe(By.XPATH, xpath)
            if btn:
                self.human_click(btn)
                print("  ✓ Clicked Save Info button")
                self.human_delay(0.5, 1.0)
                return True
        return False
    
    def _find_username_field(self):
        self.human_delay(1.5, 3.0)
        username_input = self.find_element_safe(By.CSS_SELECTOR, 'input[name="username"]')
        if not username_input:
            username_input = self.find_element_safe(By.CSS_SELECTOR, 'input[name="email"]')
        return username_input
    
    def _enter_username(self, username_input):
        if not username_input:
            print("  ✓ Username field not found (email may be stored)")
            return None
        self.human_click(username_input)
        self.human_delay(0.5, 1.0)
        username_input.clear()
        self.human_type(username_input, self.account.username, min_delay=0.08, max_delay=0.20)
        print(f"Entered username: {self.account.username}")
        self.human_delay(0.8, 1.8)
        return username_input
    
    def _handle_continue_button(self):
        continue_selectors = [
            '//button[contains(text(), "Continue")]',
            '//div[@aria-label="Continue"]',
            '//div[contains(@aria-label, "Continue")][@role="button"]',
            '//button[contains(@aria-label, "Continue")]',
        ]
        
        for selector in continue_selectors:
            btn = self.find_element_safe(By.XPATH, selector)
            if btn:
                self.human_click(btn)
                print("  ✓ Clicked Continue button")
                self.human_delay(2.0, 3.5)
                return True
        return False
    
    def _find_password_field(self, username_input, wait_for_field=False):
        if wait_for_field:
            for i in range(3):
                password_input = self.find_element_safe(By.CSS_SELECTOR, 'input[name="password"]')
                if not password_input:
                    password_input = self.find_element_safe(By.CSS_SELECTOR, 'input[name="pass"]')
                
                if password_input:
                    break
                
                if i < 2:
                    print(f"  Waiting for password field to appear (attempt {i+1}/3)...")
                    self.human_delay(1.0, 1.5)
        else:
            use_tab = __import__('random').choice([True, False])
            
            if use_tab and username_input:
                username_input.send_keys(Keys.TAB)
                self.human_delay(0.3, 0.7)
                password_input = self.driver.switch_to.active_element
                print("  ✓ Navigated to password field with Tab key")
                return password_input
            
            password_input = self.find_element_safe(By.CSS_SELECTOR, 'input[name="password"]')
            if not password_input:
                password_input = self.find_element_safe(By.CSS_SELECTOR, 'input[name="pass"]')
        
        if password_input:
            self.human_click(password_input)
            self.human_delay(0.5, 1.0)
        
        return password_input
    
    def _enter_password(self, password_input):
        password_input.clear()
        self.human_type(password_input, self.account.password, min_delay=0.06, max_delay=0.18)
        print("Entered password")
        self.human_delay(1.0, 2.5)
    
    def _submit_login_form(self, password_input):
        submit_method = __import__('random').choice(['enter', 'button'])
        
        if submit_method == 'enter':
            password_input.send_keys(Keys.RETURN)
            print("Login form submitted (Enter key)")
        else:
            login_button = self.find_element_safe(By.XPATH, '//button[@type="submit"]')
            if login_button:
                self.human_click(login_button)
                print("Login form submitted (button click)")
            else:
                password_input.send_keys(Keys.RETURN)
                print("Login form submitted (Enter key - fallback)")
        
        self.human_delay(2.0, 4.0)
    
    def _attempt_login_with_retry(self, max_retries=3):
        for attempt in range(1, max_retries + 1):
            print(f"\nLogin attempt {attempt}/{max_retries}")
            
            continue_clicked = self._handle_continue_button()
            
            if not continue_clicked:
                username_input = self._find_username_field()
                username_input = self._enter_username(username_input)
                continue_clicked = self._handle_continue_button()
            
            password_input = self._find_password_field(None, wait_for_field=continue_clicked)
            if not password_input:
                print("  ! Could not find password field")
                if attempt < max_retries:
                    print("  Retrying...")
                    self.human_delay(2.0, 3.0)
                continue
            
            self._enter_password(password_input)
            self._submit_login_form(password_input)
            
            self._handle_save_info_button()
            
            if not self._is_on_login_page():
                print("  ✓ Successfully left login page")
                return True
            
            print(f"  ! Still on login page after attempt {attempt}")
            if attempt < max_retries:
                print("  Retrying with same credentials...")
                self.human_delay(2.0, 3.0)
        
        return False
    
    def _finalize_login(self):
        print("\n" + "=" * 50)
        print("If 2FA is required, please complete it manually.")
        print("You have 5 minutes to complete the login process.")
        print("Login status will be checked every 15 seconds.")
        print("=" * 50 + "\n")
        
        success = self.wait_for_manual_action(timeout=300, check_interval=15)
        
        if success:
            print("Login appears successful!")
            self.save_cookies()
            self.account.mark_logged_in()
            print(f"Successfully logged in to Instagram as {self.account.username}")
            self.close_browser()
            return True
        else:
            print("Login timeout - please check if login was successful")
            self.save_cookies()
            if self.is_logged_in():
                self.account.mark_logged_in()
                self.close_browser()
                return True
            else:
                self.account.mark_logged_out()
                self.close_browser()
                return False
    
    def login(self):
        driver = self.start_browser()
        
        print(f"Launching Instagram for {self.account.username}...")
        print("  ✓ Using anti-detection measures")
        
        self.human_delay(1.0, 2.5)
        driver.get("https://www.instagram.com/")
        self.human_delay(2.0, 4.0)
        
        self.move_mouse_random()
        self._handle_cookie_consent()
        self.random_scroll()
        self.human_delay(1.0, 2.0)
        
        print("\nChecking if already logged in...")
        if self.is_logged_in():
            print(f"✅ Already logged in as {self.account.username}!")
            self.save_cookies()
            self.account.mark_logged_in()
            self.close_browser()
            return True
        
        print("Not logged in. Proceeding with login...\n")
        
        try:
            if not self._attempt_login_with_retry(max_retries=3):
                print("\n! All login attempts failed")
            
            return self._finalize_login()
        except Exception as e:
            print(f"Error during Instagram login: {str(e)}")
            self.account.mark_logged_out()
            if self.driver:
                self.close_browser()
            return False
