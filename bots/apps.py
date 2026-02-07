from django.apps import AppConfig


class BotsConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'bots'

    def ready(self):
        """
        Called when Django starts, after the registry is ready.
        Safe place to run initialization code that might touch models.
        """
        import sys
        import os
        
        # Only initialize for runserver/production, skip for management commands
        is_management_command = len(sys.argv) > 1 and sys.argv[1] not in ['runserver', 'test']
        
        if not is_management_command:
            from bots.services.logger import setup_telemetry
            from django.conf import settings
            
            setup_telemetry(
                service_name="login-bot",
                service_version="0.1.0",
                otlp_endpoint=os.getenv("OTLP_ENDPOINT", "https://in-otel.logs.betterstack.com/v1/traces"),
                otlp_headers={"Authorization": f"Bearer {os.getenv('BETTERSTACK_SOURCE_TOKEN', '')}"} if os.getenv('BETTERSTACK_SOURCE_TOKEN') else None,
                enable_django=True,
                enable_botocore=True,
                enable_requests=True,
                enable_celery=True,
            )
