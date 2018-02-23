# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field

class InfocifItem(Item):
    name 			= Field()
    cif 			= Field()
    url 			= Field()
    sector 			= Field()
    domicilio 		= Field()
    antiguedad 		= Field()
    fecha_constit	= Field()
    empleados 		= Field()
    cuentas 		= Field()

# class InfocifCuentas(Item):
#     year 			= Field()
#     Ingresos 		= Field()
#     EBITDA 			= Field()
#     Resultado 		= Field()
#     Tot_activo 		= Field()
#     Patrimonio_neto = Field()
#     Tot_deuda 		= Field()
#     Clientes 		= Field()
#     Proveedores 	= Field()
#     Last_year		= Field()