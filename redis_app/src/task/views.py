from django.shortcuts import render, HttpResponse
from task.tasks import update_data_base
# Create your views here.

def task_download(request):
    update_data_base.delay()
    return HttpResponse("Updating database.")