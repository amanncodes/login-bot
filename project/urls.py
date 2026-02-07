"""
URL configuration for project project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/6.0/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path
from bots.integrations import webhook

urlpatterns = [
    path('admin/', admin.site.urls),
    # Webhook endpoints for cookie provider
    path('webhook/trigger-job/', webhook.trigger_job, name='trigger_job'),
    path('webhook/release-cookie/', webhook.release_cookie, name='release_cookie'),
    path('webhook/health/', webhook.health_check, name='health_check'),
]
