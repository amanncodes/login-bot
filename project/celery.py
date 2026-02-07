"""
Celery configuration for the login-bot project.
Sets up Redis as the broker and defines platform-specific queues.
"""
import os
from celery import Celery
from celery.signals import worker_process_init
from kombu import Queue

# Set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'project.settings')

app = Celery('login_bot')

# Using a string here means the worker doesn't have to serialize
# the configuration object to child processes.
# - namespace='CELERY' means all celery-related configuration keys
#   should have a `CELERY_` prefix.
app.config_from_object('django.conf:settings', namespace='CELERY')

# Define platform-specific queues
app.conf.task_queues = (
    Queue('instagram_queue', routing_key='instagram'),
    Queue('linkedin_queue', routing_key='linkedin'),
    Queue('twitter_queue', routing_key='twitter'),
)

# Default queue configuration
app.conf.task_default_queue = 'default'
app.conf.task_default_exchange = 'default'
app.conf.task_default_routing_key = 'default'

# Load task modules from all registered Django apps.
app.autodiscover_tasks()


@worker_process_init.connect
def init_telemetry_for_worker(**kwargs):
    """
    Initialize OpenTelemetry when Celery worker process starts.
    This runs in each worker process, ensuring telemetry is available.
    """
    from bots.services.logger import setup_telemetry
    
    setup_telemetry(
        service_name="login-bot-worker",
        service_version="0.1.0",
        otlp_endpoint=os.getenv("OTLP_ENDPOINT", "https://in-otel.logs.betterstack.com/v1/traces"),
        otlp_headers={"Authorization": f"Bearer {os.getenv('BETTERSTACK_SOURCE_TOKEN', '')}"} if os.getenv('BETTERSTACK_SOURCE_TOKEN') else None,
        enable_django=True,
        enable_botocore=True,
        enable_requests=True,
        enable_celery=True,
    )


@app.task(bind=True, ignore_result=True)
def debug_task(self):
    print(f'Request: {self.request!r}')
