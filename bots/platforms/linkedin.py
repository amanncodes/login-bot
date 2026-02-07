from .base import BaseBot
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
import time

class LinkedInBot(BaseBot):
    
    def is_logged_in(self):
        """LinkedIn-specific login detection with multiple verification methods"""
        try:
            # Method 1: Check for li_at cookie (LinkedIn's session cookie)
            cookies = self.driver.get_cookies()
            has_li_at = any(cookie['name'] == 'li_at' for cookie in cookies)
            if has_li_at:
                print("    ✓ LinkedIn session cookie detected")
                return True
            
            # Method 2: Check if we're still on login page
            current_url = self.driver.current_url
            if '/login' in current_url or '/uas/login' in current_url:
                print("    ✗ Still on login page")
                return False
            
            # Method 3: Check for logged-in elements (navigation bar, feed)
            try:
                logged_in_indicators = [
                    (By.CSS_SELECTOR, '.global-nav'),
                    (By.CSS_SELECTOR, '[data-control-name="nav.settings"]'),
                    (By.ID, 'global-nav'),
                    (By.CSS_SELECTOR, '.feed-shared-update-v2'),
                ]
                
                for by, selector in logged_in_indicators:
                    try:
                        self.driver.find_element(by, selector)
                        print(f"    ✓ Logged-in navigation element found")
                        return True
                    except NoSuchElementException:
                        continue
            except Exception:
                pass
            
            # Method 4: Check URL patterns for logged-in state
            logged_in_patterns = ['/feed/', '/mynetwork/', '/jobs/', '/messaging/']
            if any(pattern in current_url for pattern in logged_in_patterns):
                print("    ✓ On logged-in page")
                return True
            
            return False
            
        except Exception as e:
            print(f"    ! Error checking login status: {str(e)}")
            return False
    def login(self):
        """Login to LinkedIn with 2FA support and cookie storage"""
        driver = self.start_browser()
        driver.get("https://www.linkedin.com/login")
        
        print(f"Launching LinkedIn for {self.account.username}...")
        
        # Wait for the page to load
        time.sleep(3)
        
        # Check if already logged in
        print("\nChecking if already logged in...")
        if self.is_logged_in():
            print(f"✅ Already logged in as {self.account.username}!")
            self.save_cookies()
            self.account.mark_logged_in()
            self.close_browser(delay=2)
            return True
        
        print("Not logged in. Proceeding with login...\n")
        
        try:
            # Wait for username input field - specifically target input tag with id="username"
            username_input = self.wait_for_element(By.CSS_SELECTOR, 'input#username', timeout=10)
            
            # Fill in username
            username_input.clear()
            username_input.send_keys(self.account.username)
            print(f"Entered username: {self.account.username}")
            
            # Fill in password - specifically target input tag with id="password"
            password_input = driver.find_element(By.CSS_SELECTOR, 'input#password')
            password_input.clear()
            password_input.send_keys(self.account.password)
            print("Entered password")
            
            # Submit the form
            password_input.send_keys(Keys.RETURN)
            print("Login form submitted")
            
            # Wait for potential 2FA or login completion (5 minutes)
            print("\n" + "="*50)
            print("If 2FA is required, please complete it manually.")
            print("You have 5 minutes to complete the login process.")
            print("Login status will be checked every 15 seconds.")
            print("="*50 + "\n")
            
            # Wait for manual action (2FA, etc.) - polls every 15 seconds
            success = self.wait_for_manual_action(timeout=300, check_interval=15)
            
            if success:
                print("Login appears successful!")
                
                # Save cookies after successful login
                self.save_cookies()
                self.account.mark_logged_in()
                print(f"Successfully logged in to LinkedIn as {self.account.username}")
                
                # Close browser after successful login
                self.close_browser()
                return True
            else:
                print("Login timeout - please check if login was successful")
                # Still try to save cookies in case login succeeded
                self.save_cookies()
                # Check one more time if actually logged in
                if self.is_logged_in():
                    self.account.mark_logged_in()
                    self.close_browser()
                    return True
                else:
                    self.account.mark_logged_out()
                    self.close_browser()
                    return False
                
        except Exception as e:
            print(f"Error during LinkedIn login: {str(e)}")
            self.account.mark_logged_out()
            if self.driver:
                self.close_browser()
            raise
