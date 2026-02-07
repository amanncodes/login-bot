from django.contrib import admin
from django.utils.html import format_html
from django.utils.safestring import mark_safe
from django.utils import timezone
from django.urls import reverse, path
from django.shortcuts import redirect
from django.contrib import messages
from .models import SocialAccount
from .platforms.instagram import InstagramBot
from .platforms.linkedin import LinkedInBot
import threading

# Global lock to ensure only one login at a time
login_lock = threading.Lock()
active_login_account = None


@admin.action(description="❌ Mark as Logged Out")
def mark_logged_out(modeladmin, request, queryset):
    """Manually mark accounts as logged out"""
    count = queryset.update(logged_in=False)
    modeladmin.message_user(request, f"Marked {count} account(s) as logged out")


@admin.action(description="🔓 Release Cookie (Mark Available)")
def release_cookie_action(modeladmin, request, queryset):
    """Manually release cookies by marking them as not in use"""
    # Update all selected cookies to not in use
    updated = queryset.update(in_use=False, last_used_at=timezone.now())
    modeladmin.message_user(
        request, 
        f"Released {updated} cookie(s) - marked as available", 
        messages.SUCCESS
    )


@admin.register(SocialAccount)
class SocialAccountAdmin(admin.ModelAdmin):
    list_display = [
        "id",
        "platform_badge",
        "username",
        "login_status",
        "usage_status",
        "failure_count",
        "last_login_display",
        "login_action"
    ]
    list_filter = ["platform", "logged_in", "in_use", "created_at"]
    search_fields = ["username"]
    readonly_fields = ["logged_in", "last_login", "in_use", "last_used_at", "consecutive_failures", "failure_reason", "created_at", "updated_at"]
    actions = [mark_logged_out, release_cookie_action]

    fieldsets = (
        ("Account Information", {"fields": ("platform", "username", "password")}),
        (
            "Login Status",
            {"fields": ("logged_in", "last_login"), "classes": ("collapse",)},
        ),
        (
            "Usage Tracking",
            {"fields": ("in_use", "last_used_at"), "classes": ("collapse",)},
        ),
        (
            "Failure Tracking",
            {"fields": ("consecutive_failures", "failure_reason"), "classes": ("collapse",)},
        ),
        (
            "Timestamps",
            {"fields": ("created_at", "updated_at"), "classes": ("collapse",)},
        ),
    )

    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path(
                "<int:account_id>/launch-login/",
                self.admin_site.admin_view(self.launch_login_view),
                name="bots_socialaccount_launch_login",
            ),
        ]
        return custom_urls + urls

    def launch_login_view(self, request, account_id):
        """View to handle individual account login"""
        global active_login_account

        # Check if a login is already in progress
        if not login_lock.acquire(blocking=False):
            messages.error(
                request,
                f"⚠️ Another login is already in progress for {active_login_account}. "
                "Please wait until it completes.",
            )
            return redirect("admin:bots_socialaccount_changelist")

        account = SocialAccount.objects.get(pk=account_id)
        active_login_account = account.username

        def run_login():
            global active_login_account
            try:
                if account.platform == "IG":
                    bot = InstagramBot(account)
                    success = bot.login()
                    if success:
                        account.mark_logged_in()
                elif account.platform == "LI":
                    bot = LinkedInBot(account)
                    success = bot.login()
                    if success:
                        account.mark_logged_in()
                else:
                    print(f"Platform {account.platform} not yet implemented")
            except Exception as e:
                print(f"Error logging in {account}: {str(e)}")
                account.mark_logged_out()
            finally:
                # Always release the lock when done
                active_login_account = None
                login_lock.release()

        # Run login in a separate thread
        thread = threading.Thread(target=run_login)
        thread.daemon = True
        thread.start()

        messages.success(
            request, f"🚀 Launching login browser for {account.username}..."
        )
        return redirect("admin:bots_socialaccount_changelist")

    def login_action(self, obj):
        """Display login button for each row"""
        url = reverse("admin:bots_socialaccount_launch_login", args=[obj.pk])
        return mark_safe(
            f'<a class="button" href="{url}" style="'
            "background-color: #417690; color: white; padding: 5px 12px; "
            "border-radius: 4px; text-decoration: none; display: inline-block; "
            "font-size: 12px; font-weight: bold;"
            '">🚀 Launch Login</a>'
        )

    login_action.short_description = "Actions"

    def platform_badge(self, obj):
        """Display platform with colored badge"""
        colors = {
            "IG": "#E4405F",  # Instagram pink
            "LI": "#0A66C2",  # LinkedIn blue
            "TW": "#1DA1F2",  # Twitter blue
        }
        color = colors.get(obj.platform, "#6c757d")
        return format_html(
            '<span style="background-color: {}; color: white; padding: 3px 10px; '
            'border-radius: 3px; font-weight: bold; font-size: 11px;">{}</span>',
            color,
            obj.get_platform_display(),
        )

    platform_badge.short_description = "Platform"

    def login_status(self, obj):
        """Display login status with colored indicator"""
        if obj.logged_in:
            return mark_safe(
                '<span style="color: #28a745; font-weight: bold;">✓ Logged In</span>'
            )
        else:
            return mark_safe(
                '<span style="color: #dc3545; font-weight: bold;">✗ Logged Out</span>'
            )

    login_status.short_description = "Status"

    def usage_status(self, obj):
        """Display usage status with colored indicator"""
        if obj.in_use:
            return mark_safe(
                '<span style="color: #ffc107; font-weight: bold;">⚡ In Use</span>'
            )
        else:
            return mark_safe(
                '<span style="color: #6c757d;">⚪ Available</span>'
            )

    usage_status.short_description = "Usage"

    def failure_count(self, obj):
        """Display failure count with warning colors"""
        if obj.consecutive_failures == 0:
            return mark_safe('<span style="color: #28a745;">0</span>')
        elif obj.consecutive_failures < 3:
            return mark_safe(
                f'<span style="color: #ffc107; font-weight: bold;" title="{obj.failure_reason or "Unknown"}">{obj.consecutive_failures}</span>'
            )
        elif obj.consecutive_failures < 5:
            return mark_safe(
                f'<span style="color: #fd7e14; font-weight: bold;" title="{obj.failure_reason or "Unknown"}">{obj.consecutive_failures} ⚠️</span>'
            )
        else:
            return mark_safe(
                f'<span style="color: #dc3545; font-weight: bold;" title="{obj.failure_reason or "Unknown"}">{obj.consecutive_failures} 🚫</span>'
            )

    failure_count.short_description = "Failures"

    def last_login_display(self, obj):
        """Display last login time in a user-friendly format"""
        if not obj.last_login:
            return mark_safe('<span style="color: #6c757d;">Never</span>')

        # Calculate time difference
        now = timezone.now()
        diff = now - obj.last_login

        if diff.days > 0:
            time_str = f"{diff.days} day{'s' if diff.days != 1 else ''} ago"
        elif diff.seconds > 3600:
            hours = diff.seconds // 3600
            time_str = f"{hours} hour{'s' if hours != 1 else ''} ago"
        elif diff.seconds > 60:
            minutes = diff.seconds // 60
            time_str = f"{minutes} minute{'s' if minutes != 1 else ''} ago"
        else:
            time_str = "Just now"

        return format_html(
            '<span title="{}">{}</span>',
            obj.last_login.strftime("%Y-%m-%d %H:%M:%S"),
            time_str,
        )

    last_login_display.short_description = "Last Login"
