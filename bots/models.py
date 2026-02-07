from django.db import models
from django.utils import timezone
import os

PLATFORM_CHOICES = (
    ("IG", "Instagram"),
    ("LI", "LinkedIn"),
    ("TW", "Twitter"),
)


class SocialAccount(models.Model):
    platform = models.CharField(max_length=2, choices=PLATFORM_CHOICES)
    username = models.CharField(max_length=255)
    password = models.CharField(max_length=255)
    logged_in = models.BooleanField(default=False, help_text="Current login status")
    last_login = models.DateTimeField(
        null=True, blank=True, help_text="Last successful login time"
    )
    cookies = models.JSONField(
        null=True, blank=True, default=dict, help_text="Browser cookies for maintaining session"
    )
    cookies_updated_at = models.DateTimeField(
        null=True, blank=True, help_text="Last time cookies were updated"
    )
    in_use = models.BooleanField(
        default=False, help_text="Whether this cookie is currently being used"
    )
    last_used_at = models.DateTimeField(
        null=True, blank=True, help_text="Last time this cookie was provided/used"
    )
    last_validated_at = models.DateTimeField(
        null=True, blank=True, help_text="Last time this cookie was validated before allocation"
    )
    consecutive_failures = models.IntegerField(
        default=0, help_text="Number of consecutive failures for this cookie"
    )
    failure_reason = models.CharField(
        max_length=255, null=True, blank=True, help_text="Reason for last cookie failure"
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def get_profile_path(self):
        # Result: selenium_profiles/instagram/user_1_myuser
        base_dir = os.path.join(os.getcwd(), "selenium_profiles")
        platform_name = self.get_platform_display().lower()
        folder_name = f"user_{self.id}_{self.username}"
        return os.path.join(base_dir, platform_name, folder_name)

    def mark_logged_in(self):
        """Mark account as logged in and update timestamp"""
        self.logged_in = True
        self.last_login = timezone.now()
        # Reset failures on successful login
        self.consecutive_failures = 0
        self.failure_reason = None
        self.save(update_fields=["logged_in", "last_login", "consecutive_failures", "failure_reason"])

    def update_cookies(self, cookies_list):
        """Update cookies in database"""
        self.cookies = cookies_list
        self.cookies_updated_at = timezone.now()
        self.save(update_fields=["cookies", "cookies_updated_at"])

    def mark_logged_out(self, reason=None):
        """Mark account as logged out"""
        self.logged_in = False
        if reason:
            self.failure_reason = reason
        self.save(update_fields=["logged_in", "failure_reason"])
    
    def increment_failures(self, reason=None):
        """Increment consecutive failures and potentially ban the account"""
        self.consecutive_failures += 1
        if reason:
            self.failure_reason = reason
        
        # Ban after 3 consecutive failures
        if self.consecutive_failures >= 3:
            self.mark_logged_out(reason=f"Banned after 3 failures: {reason}")
        
        self.save(update_fields=["consecutive_failures", "failure_reason"])
    
    def reset_failures(self):
        """Reset consecutive failures counter on successful use"""
        if self.consecutive_failures > 0:
            self.consecutive_failures = 0
            self.failure_reason = None
            self.save(update_fields=["consecutive_failures", "failure_reason"])

    def __str__(self):
        return f"[{self.get_platform_display()}] {self.username}"

    class Meta:
        unique_together = ('username', 'platform')
