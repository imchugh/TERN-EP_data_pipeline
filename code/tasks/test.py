# -*- coding: utf-8 -*-

from .registry import register, SITE_TASKS, NETWORK_TASKS

@register
def cleanup():
    print('Cleaning network')

@register
def import_site_data(site):
    print('Importing site data')

print('Site tasks:', SITE_TASKS)
print('Site tasks:', NETWORK_TASKS)
