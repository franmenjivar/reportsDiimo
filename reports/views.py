from django.shortcuts import render, redirect
from django.contrib.auth.mixins import LoginRequiredMixin
from django.views.generic import View
from django.contrib.auth import authenticate, login, logout
from django.contrib import messages
from .queries import ReportsQueries
from datetime import  timedelta, datetime
from decouple import config

# Create your views here.
DATE_FORMAT = '%Y-%m-%d'
class LandingPage(View):
    def get(self, request,*args,**kwargs):
        if request.user.is_authenticated:
            return redirect('home')
        else:
            return render(request,'reports/login.html',{})

    def post(self,request,*args,**kwargs):
        username = request.POST.get('username')
        password = request.POST.get('password')
        user = authenticate(request, username=username,password=password)

        if user is not None:
            login(request, user)
            return redirect('home')
        else:
            messages.info(request,"Username or password is incorrect")
        return render(request,'reports/login.html',{})
    
class Logout(View):
    def get(self, request, *args, **kwargs):
        logout(request)
        return redirect('login_landing')
    
class Home(LoginRequiredMixin, View):
    login_url = 'bidiimologin/'
    redirect_field_name = 'redirect_to'
    def get(self, request, *args, **kwargs):
        context = {}
        return render(request,'reports/home.html',context)
    
class Documentacion(LoginRequiredMixin,View):
    def get(self, request, *args,**kwargs):
        context={}
        return render(request,'reports/documentacion.html',context)

class DesmbolsosPorMes(LoginRequiredMixin, View):
    
    def get(self, request, *args,**kwargs):
        context={}
        return render(request,'reports/desembolsos.html',context)
    
    def post(self, request,*args,**kwargs):
        try:
            fecha_inicio = datetime.strptime(request.POST.get('fecha_inicio'),DATE_FORMAT)
            fecha_final = datetime.strptime(request.POST.get('fecha_final'),DATE_FORMAT)+timedelta(days=1)
            tipo_archivo = request.POST.get('tipo_archivo')
            nombre_archivo = request.POST.get('nombre_archivo')
            extraction_object = ReportsQueries()
            df = extraction_object.reporteDesembolsos(fecha_inicio,fecha_final)
            destination = config('DESTINATION_FOLDER')
            if tipo_archivo == '1':
                df.write_csv(f"{destination}/{nombre_archivo}.csv")
            else:
                df.write_excel(f"{destination}/{nombre_archivo}.xlsx",autofit=True)
        except:
            messages.info(request, 'Ingresa los campos correctos')
        return render(request,'reports/desembolsos.html',{})
    
class CXC(LoginRequiredMixin,View):
    def get(self, request, *args, **kwargs):
        context={}
        return render(request, 'reports/cxc.html', context)
    
    def post(self, request,*args,**kwargs):
        try:
            fecha_inicio = datetime.strptime(request.POST.get('fecha_inicio'),DATE_FORMAT)
            tipo_archivo = request.POST.get('tipo_archivo')
            nombre_archivo = request.POST.get('nombre_archivo')
            extraction_object = ReportsQueries()
            df = extraction_object.reportecxc(fecha_inicio)
            destination = config('DESTINATION_FOLDER')

            if tipo_archivo == '1':
                df.write_csv(f"{destination}/{nombre_archivo}.csv")
            else:
                df.write_excel(f"{destination}/{nombre_archivo}.xlsx",autofit=True)
        except:
            messages.info(request, 'Ingresa los campos correctos')
        return render(request,'reports/cxc.html',{})
