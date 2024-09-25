from django.urls import path
from .views import *

urlpatterns = [
    path('',Home.as_view(), name='home'),
    path('logout/',Logout.as_view(), name='logout'),
    path('bidiimologin/',LandingPage.as_view(), name='login_landing'),
    path('documentacion/',Documentacion.as_view(), name='docs'),
    path('desembolsos/',DesmbolsosPorMes.as_view(), name='desem'),
    path('CXC/',CXC.as_view(), name='CXC'),
]
