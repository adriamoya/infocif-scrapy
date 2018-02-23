
import re
import os.path
import csv
import scrapy

from scrapy.conf import settings
from scrapy.spiders import CrawlSpider, Rule
from scrapy.http.request import Request

from scrapy_splash import SplashRequest

from infocif.items import InfocifItem


lookup_url  = 'http://www.infocif.es/general/empresas-informacion-listado-empresas.asp?Buscar='
general_url = 'http://www.infocif.es/ficha-empresa/'
cuentas_url = 'http://www.infocif.es/balance-cuentas-anuales/'

# cif_input = 'B86561412'


start_urls_list = []
with open(os.path.dirname(__file__) + '/../inputs/cifs.csv', 'rb') as f:
    reader = csv.reader(f)
    for row in reader:
        start_urls_list.append(lookup_url+row[0])

# B Create a named spider
class InfocifSpider(scrapy.Spider):

	name = 'infocif3'
	allowed_domains = ['infocif.es']
	start_urls = start_urls_list
	# start_urls = [
	# 	general_url+cif_input,
	# ]

	def parse(self, response):

		self.logger.info('Checking existence...')

		if response.status == 200:

			not_exists = response.xpath('//div[contains(text(),"Empresa no encontrada!")]/text()').extract()

			# response.xpath('//strong[text()="Sector"]/following-sibling::*/text()')[0].extract().strip()

			if not not_exists:

				company_id = response.url.split('/')[-1]

				if company_id:

					# request = SplashRequest(
					# 	url=general_url+company_id, 
					# 	callback=self.parse_general,
					# 	args={
					# 		'wait': 30,
					# 	},
					# )

					request = scrapy.Request(
						general_url+company_id,
						callback=self.parse_general,
						dont_filter=True)

					request.meta['company_id'] = company_id

					yield request

			else:
				pass

		else:
			pass


	def parse_general(self, response):

		self.logger.info('Parsing general...')

		if response.status == 200:

			self.logger.info(response.xpath('//*[contains(text(),"ACERO")]'))

			company_id = response.meta['company_id']

			company = InfocifItem()

			# url
			# -------------------------------------------------
			try:
				url = response.url
				if url:
					self.logger.info(url)
					company['url'] = url
			except:
				pass

			# name
			# -------------------------------------------------
			try:
				name = response.xpath('//div[contains(@class,"casocabecera")]//h1/text()').extract()[0].encode('ascii','ignore')
				if name:
					self.logger.info(name)
					company['name'] = name
			except:
				pass


			# panel de informacion
			# temp = response.xpath('//div[contains(@id,"fe-informacion-izq")]')[0]

			# cif
			# -------------------------------------------------
			try:
				cif = response.xpath('//strong[contains(text(),"CIF")]/following-sibling::*/text()')[0].extract().strip()
				if cif:
					self.logger.info(cif)
					company['cif'] = cif
			except:
				pass

			# antiguedad
			# -------------------------------------------------
			try:
				antiguedad = response.xpath('//strong[contains(text(),"Antig")]/following-sibling::*/text()')[0].extract().strip()
				year_creacion = re.findall('[0-9]+', antiguedad)[-1].encode('ascii','ignore')
				month_creacion = re.findall('[0-9]+', antiguedad)[-2].encode('ascii','ignore')
				antiguedad = re.findall('[0-9]+', antiguedad)[0].encode('ascii','ignore')
				if antiguedad:
					self.logger.info(antiguedad)
					company['antiguedad'] = antiguedad
				if year_creacion and month_creacion:
					company['fecha_constit'] = year_creacion + month_creacion
			except:
				pass

			# sector
			# -------------------------------------------------
			try:
				sector = response.xpath('//strong[text()="Sector"]/following-sibling::*/text()')[0].extract().strip()
				if sector:
					self.logger.info(sector)
					company['sector'] = sector
			except:
				pass

			# domicilio
			# -------------------------------------------------
			try:
				domicilio =  response.xpath('//strong[contains(text(),"Domicilio")]/following-sibling::*/text()')[0].extract().strip()
				if domicilio:
					self.logger.info(domicilio)
					company['domicilio'] = domicilio
			except:
				pass

			# n empleados
			# -------------------------------------------------
			try:
				empleados = response.xpath('//strong[contains(text(),"empleados")]/following-sibling::*/text()')[0].extract().strip()
				if empleados:
					self.logger.info(empleados)
					company['empleados'] = empleados
			except:
				pass


			company_id = url.split('/')[-1]

			if company_id:
				request = scrapy.Request(
					cuentas_url+company_id,
					callback=self.parse_cuentas,
					dont_filter=True)
				request.meta['item'] = company

				yield request

		else:
			pass



	def parse_cuentas(self, response):

		self.logger.info('Parsing cuentas...')

		company = response.meta['item']

		try:
			
			# determinar si las cuentas estan en miles o no
			miles = response.xpath('//th[contains(text(),"Cuenta de resultados")]/text()')[0].extract()
			miles = re.findall('(miles)', miles)

			year_list				= response.xpath('//th[contains(text(),"Cuenta de resultados")]')[0].xpath('./following-sibling::th/span/text()')
			ingresos_expl_list  	= response.xpath('//td[contains(text(),"Ingresos de expl")]/following-sibling::td/text()')
			amortizaciones_list 	= response.xpath('//td[contains(text(),"Amortiz")]/following-sibling::td/span/text()')
			
			resultado_expl_list 	= response.xpath('//td[contains(text(),"Resultado de explotaci")]/following-sibling::td')
			total_activo_list 		= response.xpath('//td[contains(text(),"Total activo")]/following-sibling::td/text()')
			patrimonio_neto_list 	= response.xpath('//td[contains(text(),"Patrimonio neto")]/following-sibling::td/text()')
			deudas_cp_list			= response.xpath('//td[contains(text(),"Deudas a corto plazo")]/following-sibling::td/text()')
			deudas_lp_list			= response.xpath('//td[contains(text(),"Deudas a largo plazo")]/following-sibling::td/text()')
			deudores_comerc_list 	= response.xpath('//td[contains(text(),"Deudores comerciales")]/following-sibling::td/text()')
			acreedores_comerc_list 	= response.xpath('//td[contains(text(),"Acreedores comerciales")]/following-sibling::td/text()')


			if year_list:

				cuentas = []

				# para cada ejercicio
				for i in range(0, len(year_list), 1):

					year = int(year_list[i].extract())

					if miles:
 
						# cuenta de resultados
						# --------------------------------------------------------

						# ingresos de explotacion
						ingresos_expl = ''.join(re.findall('[0-9]',ingresos_expl_list[i].extract()))
						if ingresos_expl:
							ingresos_expl = float(ingresos_expl)*1000
						else:
							ingresos_expl = 0
						
						#amortizaciones
						amortizaciones = ''.join(re.findall('[0-9]',amortizaciones_list[i].extract()))
						if amortizaciones:
							amortizaciones = float(amortizaciones)*1000
						else:
							amortizaciones = 0
						
						# resultado de explotacion
						if resultado_expl_list[i].xpath('./span[contains(@class,"rojo")]'):
							# negativo
							resultado_expl = ''.join(re.findall('[0-9]',resultado_expl_list[i].xpath('./span/text()').extract()[0]))
							if resultado_expl:
								resultado_expl = float(resultado_expl)*(-1000)
							else:
								resultado_expl = 0
						else:
							# positivo
							resultado_expl = ''.join(re.findall('[0-9]',resultado_expl_list[i].xpath('./text()').extract()[0]))
							if resultado_expl:
								resultado_expl = float(resultado_expl)*(1000)
							else:
								resultado_expl = 0

						# ebitda
						ebitda 				= resultado_expl + amortizaciones

						# balance
						# --------------------------------------------------------

						# total activo
						total_activo = ''.join(re.findall('[0-9]',total_activo_list[i].extract()))
						if total_activo:
							total_activo = float(total_activo)*1000
						else:
							total_activo = 0
						
						# patrimonio neto
						patrimonio_neto = ''.join(re.findall('[0-9]',patrimonio_neto_list[i].extract()))
						if patrimonio_neto:
							patrimonio_neto = float(patrimonio_neto)*1000
						else:
							patrimonio_neto = 0
						
						# deudas a corto plazo
						deudas_cp = ''.join(re.findall('[0-9]',deudas_cp_list[i].extract()))
						if deudas_cp:
							deudas_cp = float(deudas_cp)*1000
						else:
							deudas_cp = 0

						# deudas a largo plazo
						deudas_lp = ''.join(re.findall('[0-9]',deudas_lp_list[i].extract()))
						if deudas_lp:
							deudas_lp = float(deudas_lp)*1000
						else:
							deudas_lp = 0

						# deudores comerciales
						deudores_comerc = ''.join(re.findall('[0-9]',deudores_comerc_list[i].extract()))
						if deudores_comerc:
							deudores_comerc = float(deudores_comerc)*1000
						else:
							deudores_comerc = 0

						# acreedores comerciales
						acreedores_comerc = ''.join(re.findall('[0-9]',acreedores_comerc_list[i].extract()))
						if acreedores_comerc:
							acreedores_comerc = float(acreedores_comerc)*1000
						else:
							acreedores_comerc = 0

					else:

						# cuenta de resultados
						# --------------------------------------------------------
						
						# ingresos de explotacion
						ingresos_expl 		= float(''.join(re.findall('[0-9]', ingresos_expl_list[i].extract())))

						# amortizaciones
						amortizaciones 		= float(''.join(re.findall('[0-9]', amortizaciones_list[i].extract())))
						
						# resultado de explotacion
						if resultado_expl_list[i].xpath('./span[contains(@class,"rojo")]'):
							# negativo
							resultado_expl  = float(''.join(re.findall('[0-9]', resultado_expl_list[i].xpath('./span/text()').extract()[0])))*(-1)
						else:
							# postivio
							resultado_expl  = float(''.join(re.findall('[0-9]', resultado_expl_list[i].xpath('./text()').extract()[0])))
						
						# ebitda
						ebitda 				= resultado_expl + amortizaciones

						# balance
						# --------------------------------------------------------

						# total activo
						total_activo 		= float(''.join(re.findall('[0-9]', total_activo_list[i].extract())))

						# patrimonio neto
						patrimonio_neto 	= float(''.join(re.findall('[0-9]', patrimonio_neto_list[i].extract())))

						# deudas a corto plazo
						deudas_cp 			= float(''.join(re.findall('[0-9]', deudas_cp_list[i].extract()))) # deudas total

						# deudas a largo plazo
						deudas_lp 			= float(''.join(re.findall('[0-9]', deudas_lp_list[i].extract()))) # deudas total

						# deudores comerciales
						deudores_comerc		= float(''.join(re.findall('[0-9]', deudores_comerc_list[i].extract()))) # clientes

						# acreedores comerciales
						acreedores_comerc 	= float(''.join(re.findall('[0-9]', acreedores_comerc_list[i].extract()))) # proveedores


					cuenta = {
						'year': year,
						'ingresos_expl': ingresos_expl,
						'amortizaciones': amortizaciones,
						'resultado_expl': resultado_expl,
						'ebitda': ebitda,
						'total_activo': total_activo,
						'patrimonio_neto': patrimonio_neto,
						'deudas_cp': deudas_cp,
						'deudas_lp': deudas_lp,
						'deudas_total': deudas_cp + deudas_lp,
						'clientes': deudores_comerc,
						'proveedores': acreedores_comerc
					}
					self.logger.info(cuenta)

					cuentas.append(cuenta)

				company['cuentas'] = cuentas

		except:
			pass
		
		yield company