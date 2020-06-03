TComapi
========

Tcomapi is a project designed to get open data and not only for KTelecom needs. 
We have pipelines built using Luigi and api code wraped in bundle intended to work with 
some known KZ open data sites. Pipelines placed in tasks folder. 

## Pipelines

### data.egov.kz

- Types of administrative territorial units - 
[pipeline](https://github.com/elessarelfstone/tcomapi/blob/master/tasks/addressregister.py) | [link](https://data.egov.kz/datasets/view?index=d_ats_types)

- Types of elements of populated localities - 
[pipeline](https://github.com/elessarelfstone/tcomapi/blob/master/tasks/addressregister.py) | [link](https://data.egov.kz/datasets/view?index=d_geonims_types)

- Indicators of types of initial properties - 
[pipeline](https://github.com/elessarelfstone/tcomapi/blob/master/tasks/addressregister.py) | [link](https://data.egov.kz/datasets/view?index=d_buildings_pointers)

- Indicators of types of secondary properties - 
[pipeline](https://github.com/elessarelfstone/tcomapi/blob/master/tasks/addressregister.py) | [link](https://data.egov.kz/datasets/view?index=d_rooms_types)

- List of administrative territorial entities of RoK and subordinance connection between them - 
[pipeline](https://github.com/elessarelfstone/tcomapi/blob/master/tasks/addressregister.py) | [link](https://data.egov.kz/datasets/view?index=s_ats)

- List of Populated locality systems, also connection of substrcuture between them - 
[pipeline](https://github.com/elessarelfstone/tcomapi/blob/master/tasks/addressregister.py) | [link](https://data.egov.kz/datasets/view?index=s_geonims)

- List of real estates, also connection of territories with administrative territorial entity and populated locality system - 
[pipeline](https://github.com/elessarelfstone/tcomapi/blob/master/tasks/addressregister.py) | [link](https://data.egov.kz/datasets/view?index=s_grounds_new)

- Except objects contains information about connection of objects with Administrative territorial entity and populated locality system and the territory - 
[pipeline](https://github.com/elessarelfstone/tcomapi/blob/master/tasks/addressregister.py) | [link](https://data.egov.kz/datasets/view?index=s_buildings)

- Except objects contains information about connection of objects with preliminary object - 
[pipeline](https://github.com/elessarelfstone/tcomapi/blob/master/tasks/addressregister.py) | [link](https://data.egov.kz/datasets/view?index=s_pb)



### Kgd.gov.kz

 - List of taxpayers reorganized in violation of the Tax Code - 
[pipeline](https://github.com/elessarelfstone/tcomapi/blob/master/tasks/taxviolators.py) | [link](http://kgd.gov.kz/mobile_api/services/taxpayers_unreliable_exportexcel/VIOLATION_TAX_CODE/KZ_ALL/fileName/list_VIOLATION_TAX_CODE_KZ_ALL.xlsx)

- List of debtors who have unpaied taxes sum more than 150 MNU(monthly notional unit). - 
[pipeline](https://github.com/elessarelfstone/tcomapi/blob/master/tasks/taxarrears150.py) | [link](http://kgd.gov.kz/mobile_api/services/taxpayers_unreliable_exportexcel/TAX_ARREARS_150/KZ_ALL/fileName/list_TAX_ARREARS_150_KZ_ALL.xlsx)

- List of taxpayers declared bankrupt - 
[pipeline](https://github.com/elessarelfstone/tcomapi/blob/master/tasks/bankrupt.py) | [link](http://kgd.gov.kz/mobile_api/services/taxpayers_unreliable_exportexcel/BANKRUPT/KZ_ALL/fileName/list_BANKRUPT_KZ_ALL.xlsx)

- Information on taxpayers declared inactive - 
[pipeline](https://github.com/elessarelfstone/tcomapi/blob/master/tasks/inactive.py) | [link](http://kgd.gov.kz/mobile_api/services/taxpayers_unreliable_exportexcel/INACTIVE/KZ_ALL/fileName/list_INACTIVE_KZ_ALL.xlsx)

- List of taxpayers whose registration is invalid - 
[pipeline](https://github.com/elessarelfstone/tcomapi/blob/master/tasks/invregistration.py) | [link](http://kgd.gov.kz/mobile_api/services/taxpayers_unreliable_exportexcel/INVALID_REGISTRATION/KZ_ALL/fileName/list_INVALID_REGISTRATION_KZ_ALL.xlsx)

- List of taxpayers with wrong legal address - 
[pipeline](https://github.com/elessarelfstone/tcomapi/blob/master/tasks/jwaddress.py) | [link](http://kgd.gov.kz/mobile_api/services/taxpayers_unreliable_exportexcel/WRONG_ADDRESS/KZ_ALL/fileName/list_WRONG_ADDRESS_KZ_ALL.xlsx)

- List of changes in MNU(monthly notional unit) by year -
[pipeline](https://github.com/elessarelfstone/tcomapi/blob/master/tasks/mrp.py) | [link](http://kgd.gov.kz/reference/mrp)

- List of changes in minimal salary by year - 
[pipeline](https://github.com/elessarelfstone/tcomapi/blob/master/tasks/mrp.py) | [link](http://kgd.gov.kz/reference/mzp)

- List of companies declared as pseudocompanies - 
[pipeline](https://github.com/elessarelfstone/tcomapi/blob/master/tasks/pseudocompany.py) | [link](http://kgd.gov.kz/mobile_api/services/taxpayers_unreliable_exportexcel/PSEUDO_COMPANY/KZ_ALL/fileName/list_PSEUDO_COMPANY_KZ_ALL.xlsx)

- List of сhanges in refinancing rate by year - 
[pipeline](https://github.com/elessarelfstone/tcomapi/blob/master/tasks/refinance.py) | [link](http://kgd.gov.kz/mobile_api/services/taxpayers_unreliable_exportexcel/PSEUDO_COMPANY/KZ_ALL/fileName/list_PSEUDO_COMPANY_KZ_ALL.xlsx)


### Stat.gov.kz

- List of companies of Kazakhstan by regions - 
[pipeline](https://github.com/elessarelfstone/tcomapi/blob/master/tasks/companies.py) | [urltemplate](http://stat.gov.kz/api/getFile/?docId=) | [uries](http://stat.gov.kz/api/content/?objId=WC16200009069)

- Сlassifier of administrative-territorial objects - 
[pipeline](https://github.com/elessarelfstone/tcomapi/blob/master/tasks/kato.py) | [link](https://stat.gov.kz/api/getFile/?docId=ESTAT333581)

- Product classifier by types of economic activity - 
[pipeline](https://github.com/elessarelfstone/tcomapi/blob/master/tasks/kpved.py) | [link](https://stat.gov.kz/api/getFile/?docId=ESTAT116569)

- List of codes of the streets of Kazakhstan - 
[pipeline](https://github.com/elessarelfstone/tcomapi/blob/master/tasks/kurk.py) | [link](http://old.stat.gov.kz/getImg?id=WC16200004875)

- Сlassifier of units of measure and reference unit of measure - 
[pipeline](https://github.com/elessarelfstone/tcomapi/blob/master/tasks/mkeis.py) | [link](https://stat.gov.kz/api/getFile/?docId=ESTAT316776)

- General classifier of types of economic activity - 
[pipeline](https://github.com/elessarelfstone/tcomapi/blob/master/tasks/oked.py) | [link](https://stat.gov.kz/api/getFile/?docId=ESTAT310324)

