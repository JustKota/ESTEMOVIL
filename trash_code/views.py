from django.http import HttpResponse
from django.shortcuts import render


def hello_world(request):
    #return HttpResponse('Hola mundo, me c*go en todo')
    return render(request, 'home.html')


