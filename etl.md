# Create ETL

## The ETL Script
### Starting up the docker
to run the docker open any terminal and run:
```shell
# Terminal 1
docker run -itd -p 8888:8888 -p 4040:4040 -v ~/.aws:/root/.aws:ro --name glue_jupyter \amazon/aws-glue-libs:glue_libs_1.0.0_image_01 \
/home/jupyter/jupyter_start.sh
```

### Glue and spark
load context:
```python
import sys

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import *
from pyspark.sql import Row
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
```
load json
```python
json_line_1 = u'{"statementID":"openownership-register-12888532280668802411","statementType":"entityStatement","entityType":"registeredEntity","name":"ACCOUNTS \u0026 FINANCE CONSULTANTS LTD","incorporatedInJurisdiction":{"name":"United Kingdom of Great Britain and Northern Ireland","code":"GB"},"identifiers":[{"scheme":"GB-COH","schemeName":"Companies House","id":"07656492"},{"schemeName":"OpenCorporates","id":"https://opencorporates.com/companies/gb/07656492","uri":"https://opencorporates.com/companies/gb/07656492"},{"schemeName":"OpenOwnership Register","id":"http://register.openownership.org/entities/59b9194967e4ebf340d2a1b4","uri":"http://register.openownership.org/entities/59b9194967e4ebf340d2a1b4"}],"foundingDate":"2011-06-02","addresses":[{"type":"registered","address":"1000 Great West Road, Brentford, Middlesex, TW8 9HH","country":"GB"}]}'
json_line_2 = u'{"statementID":"openownership-register-8299183882784755246","statementType":"personStatement","personType":"knownPerson","names":[{"type":"individual","fullName":"Asif Rafiq"}],"identifiers":[{"schemeName":"GB Persons Of Significant Control Register","id":"/company/07656492/persons-with-significant-control/individual/RJxJK4jpeJd6xl0uv3CfJuaQydE"},{"schemeName":"OpenOwnership Register","id":"http://register.openownership.org/entities/59b9194967e4ebf340d2a1d3","uri":"http://register.openownership.org/entities/59b9194967e4ebf340d2a1d3"}],"nationalities":[{"name":"United Kingdom of Great Britain and Northern Ireland","code":"GB"}],"birthDate":"1965-10-01","addresses":[{"address":"1000, Great West Road, Brentford, Middlesex, TW8 9HH","country":"GB"}]}'
json_line_3 = u'{"statementID":"openownership-register-5919865859795084499","statementType":"ownershipOrControlStatement","statementDate":"2016-06-30","subject":{"describedByEntityStatement":"openownership-register-12888532280668802411"},"interestedParty":{"describedByPersonStatement":"openownership-register-8299183882784755246"},"interests":[{"type":"shareholding","details":"ownership-of-shares-75-to-100-percent","share":{"minimum":75,"maximum":100,"exclusiveMinimum":false,"exclusiveMaximum":false},"startDate":"2016-06-30"}],"source":{"type":["officialRegister"],"description":"GB Persons Of Significant Control Register","url":"http://download.companieshouse.gov.uk/en_pscdata.html","retrievedAt":"2019-07-08T10:37:11Z"}}'
json_line_4 = u'{"statementID":"openownership-register-13207780502341118379","statementType":"entityStatement","entityType":"registeredEntity","name":"PENSPEN THEIA LIMITED","incorporatedInJurisdiction":{"name":"United Kingdom of Great Britain and Northern Ireland","code":"GB"},"identifiers":[{"scheme":"GB-COH","schemeName":"Companies House","id":"12317095"},{"schemeName":"OpenCorporates","id":"https://opencorporates.com/companies/gb/12317095","uri":"https://opencorporates.com/companies/gb/12317095"},{"schemeName":"OpenOwnership Register","id":"http://register.openownership.org/entities/5ddee7649dfc3fae18cc2c16","uri":"http://register.openownership.org/entities/5ddee7649dfc3fae18cc2c16"}],"foundingDate":"2019-11-15","addresses":[{"type":"registered","address":"3 Water Lane, Richmond, TW9 1TJ","country":"GB"}]}'
json_line_5 = u'{"statementID":"openownership-register-632107462438244930","statementType":"entityStatement","entityType":"registeredEntity","name":"THE PENSPEN GROUP LIMITED","incorporatedInJurisdiction":{"name":"United Kingdom of Great Britain and Northern Ireland","code":"GB"},"identifiers":[{"scheme":"GB-COH","schemeName":"Companies House","id":"00980600"},{"schemeName":"OpenCorporates","id":"https://opencorporates.com/companies/gb/00980600","uri":"https://opencorporates.com/companies/gb/00980600"},{"schemeName":"GB Persons Of Significant Control Register","id":"/company/12317095/persons-with-significant-control/corporate-entity/VRfHKDSnWVqUG6MVoiA2rAWseK0"},{"schemeName":"GB Persons Of Significant Control Register - Registration numbers","id":"00980600"},{"schemeName":"OpenOwnership Register","id":"http://register.openownership.org/entities/59b9194967e4ebf340d2a31e","uri":"http://register.openownership.org/entities/59b9194967e4ebf340d2a31e"}],"foundingDate":"1970-05-27","addresses":[{"type":"registered","address":"3 Water Lane, Richmond, Surrey, TW9 1TJ","country":"GB"}]}'
json_line_6 = u'{"statementID":"openownership-register-15960189512649194231","statementType":"ownershipOrControlStatement","statementDate":"2019-11-15","subject":{"describedByEntityStatement":"openownership-register-13207780502341118379"},"interestedParty":{"describedByEntityStatement":"openownership-register-632107462438244930"},"interests":[{"type":"shareholding","details":"ownership-of-shares-75-to-100-percent","share":{"minimum":75,"maximum":100,"exclusiveMinimum":false,"exclusiveMaximum":false},"startDate":"2019-11-15"},{"type":"voting-rights","details":"voting-rights-75-to-100-percent","share":{"minimum":75,"maximum":100,"exclusiveMinimum":false,"exclusiveMaximum":false},"startDate":"2019-11-15"},{"type":"appointment-of-board","details":"right-to-appoint-and-remove-directors","startDate":"2019-11-15"}],"source":{"type":["officialRegister"],"description":"GB Persons Of Significant Control Register","url":"http://download.companieshouse.gov.uk/en_pscdata.html","retrievedAt":"2019-11-27T19:27:34Z"}}'
json_line_7 = u'{"statementID":"openownership-register-6168448609989246900","statementType":"entityStatement","entityType":"registeredEntity","name":"DAR AL-HANDASAH (UK) LIMITED","incorporatedInJurisdiction":{"name":"United Kingdom of Great Britain and Northern Ireland","code":"GB"},"identifiers":[{"schemeName":"GB Persons Of Significant Control Register","id":"/company/00980600/persons-with-significant-control/corporate-entity/lwGVNPmoGagGsloPGItEgsmlR0w"},{"schemeName":"GB Persons Of Significant Control Register - Registration numbers","id":"02453001"},{"schemeName":"GB Persons Of Significant Control Register","id":"/company/01209014/persons-with-significant-control/corporate-entity/tlaeTT6FkE4NE9X_5Pjnt_K2PLM"},{"schemeName":"GB Persons Of Significant Control Register","id":"/company/01858286/persons-with-significant-control/corporate-entity/2iHl51m-rj93CqKz_Tw-ORvC2KM"},{"scheme":"GB-COH","schemeName":"Companies House","id":"02453001"},{"schemeName":"GB Persons Of Significant Control Register","id":"/company/09044221/persons-with-significant-control/corporate-entity/orTDUrlRiKQqWs956AErOt6t76I"},{"schemeName":"GB Persons Of Significant Control Register","id":"/company/07323385/persons-with-significant-control/corporate-entity/haycNEMNhau4e4R8-7iI89fomNM"},{"schemeName":"OpenCorporates","id":"https://opencorporates.com/companies/gb/02453001","uri":"https://opencorporates.com/companies/gb/02453001"},{"schemeName":"OpenOwnership Register","id":"http://register.openownership.org/entities/59b9194a67e4ebf340d2a487","uri":"http://register.openownership.org/entities/59b9194a67e4ebf340d2a487"}],"foundingDate":"1989-12-15","addresses":[{"type":"registered","address":"74 Wigmore Street, London, W1U 2SQ","country":"GB"}]}'
json_line_8 = u'{"statementID":"openownership-register-961101174226092012","statementType":"personStatement","personType":"knownPerson","names":[{"type":"individual","fullName":"Talal Shair"}],"identifiers":[{"schemeName":"GB Persons Of Significant Control Register","id":"/company/02453001/persons-with-significant-control/individual/eTBKKo-oFp4aaNlmp0NOELV1tak"},{"schemeName":"OpenOwnership Register","id":"http://register.openownership.org/entities/59b9514367e4ebf340c60228","uri":"http://register.openownership.org/entities/59b9514367e4ebf340c60228"}],"nationalities":[{"name":"Jordan","code":"JO"}],"birthDate":"1967-07-01","addresses":[{"address":"74, Wigmore Street, London, W1U 2SQ","country":"LB"}]}'
json_line_9 = u'{"statementID":"openownership-register-7568992443177146603","statementType":"ownershipOrControlStatement","statementDate":"2016-04-06","subject":{"describedByEntityStatement":"openownership-register-6168448609989246900"},"interestedParty":{"describedByPersonStatement":"openownership-register-961101174226092012"},"interests":[{"type":"shareholding","details":"ownership-of-shares-25-to-50-percent","share":{"minimum":25,"maximum":50,"exclusiveMinimum":true,"exclusiveMaximum":false},"startDate":"2016-04-06"},{"type":"voting-rights","details":"voting-rights-25-to-50-percent","share":{"minimum":25,"maximum":50,"exclusiveMinimum":true,"exclusiveMaximum":false},"startDate":"2016-04-06"}],"source":{"type":["officialRegister"],"description":"GB Persons Of Significant Control Register","url":"http://download.companieshouse.gov.uk/en_pscdata.html","retrievedAt":"2019-07-08T10:37:11Z"}}'
json_line_10 = u'{"statementID":"openownership-register-5650137013893294758","statementType":"ownershipOrControlStatement","statementDate":"2016-04-06","subject":{"describedByEntityStatement":"openownership-register-632107462438244930"},"interestedParty":{"describedByEntityStatement":"openownership-register-6168448609989246900"},"interests":[{"type":"shareholding","details":"ownership-of-shares-75-to-100-percent","share":{"minimum":75,"maximum":100,"exclusiveMinimum":false,"exclusiveMaximum":false},"startDate":"2016-04-06"}],"source":{"type":["officialRegister"],"description":"GB Persons Of Significant Control Register","url":"http://download.companieshouse.gov.uk/en_pscdata.html","retrievedAt":"2019-07-08T10:37:11Z"}}'
json_line_11 = u'{"statementID":"openownership-register-8767626642141751059","statementType":"entityStatement","entityType":"registeredEntity","name":"ZUBER CHAUFFERS LTD","incorporatedInJurisdiction":{"name":"United Kingdom of Great Britain and Northern Ireland","code":"GB"},"identifiers":[{"scheme":"GB-COH","schemeName":"Companies House","id":"10236028"},{"schemeName":"OpenCorporates","id":"https://opencorporates.com/companies/gb/10236028","uri":"https://opencorporates.com/companies/gb/10236028"},{"schemeName":"OpenOwnership Register","id":"http://register.openownership.org/entities/59b9194967e4ebf340d2a325","uri":"http://register.openownership.org/entities/59b9194967e4ebf340d2a325"}],"foundingDate":"2016-06-16","dissolutionDate":"2019-10-08","addresses":[{"type":"registered","address":"485 Kingsland Road, Dalston, London, E8 4AU","country":"GB"}]}'
json_line_12 = u'{"statementID":"openownership-register-9556908071460773737","statementType":"personStatement","personType":"knownPerson","names":[{"type":"individual","fullName":"Nghiem Van Trong"}],"identifiers":[{"schemeName":"GB Persons Of Significant Control Register","id":"/company/10236028/persons-with-significant-control/individual/Pt4Z4uFRj2SCK3rXkQn5Cp57SJo"},{"schemeName":"OpenOwnership Register","id":"http://register.openownership.org/entities/5d399a4e9dfc3fae184c2884","uri":"http://register.openownership.org/entities/5d399a4e9dfc3fae184c2884"}],"nationalities":[{"name":"Viet Nam","code":"VN"}],"birthDate":"1969-07-01","addresses":[{"address":"128, Kingsland Road, London, E2 8DP","country":"GB"}]}'
json_line_13 = u'{"statementID":"openownership-register-13575613196921982558","statementType":"ownershipOrControlStatement","statementDate":"2019-07-12","subject":{"describedByEntityStatement":"openownership-register-8767626642141751059"},"interestedParty":{"describedByPersonStatement":"openownership-register-9556908071460773737"},"interests":[{"type":"influence-or-control","details":"significant-influence-or-control","startDate":"2019-07-12","endDate":"2019-07-12"}],"source":{"type":["officialRegister"],"description":"GB Persons Of Significant Control Register","url":"http://download.companieshouse.gov.uk/en_pscdata.html","retrievedAt":"2019-07-25T09:59:00Z"}}'
json_line_14 = u'{"statementID":"openownership-register-7100670311841309934","statementType":"personStatement","personType":"knownPerson","names":[{"type":"individual","fullName":"Nghiem Van Trong"}],"identifiers":[{"schemeName":"GB Persons Of Significant Control Register","id":"/company/10236028/persons-with-significant-control/individual/CSDnlzE-bWBpHVRfFI6CnMrMc1I"},{"schemeName":"OpenOwnership Register","id":"http://register.openownership.org/entities/5d399a639dfc3fae184c701a","uri":"http://register.openownership.org/entities/5d399a639dfc3fae184c701a"}],"nationalities":[{"name":"Viet Nam","code":"VN"}],"birthDate":"1963-07-01","addresses":[{"address":"128, Kingsland Road, London, E2 8DP","country":"GB"}]}'
json_line_15 = u'{"statementID":"openownership-register-2010631938803128446","statementType":"ownershipOrControlStatement","statementDate":"2019-07-12","subject":{"describedByEntityStatement":"openownership-register-8767626642141751059"},"interestedParty":{"describedByPersonStatement":"openownership-register-7100670311841309934"},"interests":[{"type":"influence-or-control","details":"significant-influence-or-control","startDate":"2019-07-12"}],"source":{"type":["officialRegister"],"description":"GB Persons Of Significant Control Register","url":"http://download.companieshouse.gov.uk/en_pscdata.html","retrievedAt":"2019-07-25T09:59:00Z"}}'
json_line_16 = u'{"statementID":"openownership-register-2944380461126310982","statementType":"personStatement","personType":"knownPerson","names":[{"type":"individual","fullName":"Ferdi Tekgul"}],"identifiers":[{"schemeName":"GB Persons Of Significant Control Register","id":"/company/10236028/persons-with-significant-control/individual/InA7Yho_wVlY1rV4jROt7giYYoY"},{"schemeName":"OpenOwnership Register","id":"http://register.openownership.org/entities/59b9194a67e4ebf340d2a329","uri":"http://register.openownership.org/entities/59b9194a67e4ebf340d2a329"}],"nationalities":[{"name":"Turkey","code":"TR"}],"birthDate":"1979-10-01","addresses":[{"address":"485, Kingsland Road, Dalston, London, E8 4AU"}]}'
json_line_17 = u'{"statementID":"openownership-register-14833803755324152228","statementType":"ownershipOrControlStatement","statementDate":"2016-06-30","subject":{"describedByEntityStatement":"openownership-register-8767626642141751059"},"interestedParty":{"describedByPersonStatement":"openownership-register-2944380461126310982"},"interests":[{"type":"shareholding","details":"ownership-of-shares-75-to-100-percent","share":{"minimum":75,"maximum":100,"exclusiveMinimum":false,"exclusiveMaximum":false},"startDate":"2016-06-30","endDate":"2019-07-12"},{"type":"voting-rights","details":"voting-rights-75-to-100-percent","share":{"minimum":75,"maximum":100,"exclusiveMinimum":false,"exclusiveMaximum":false},"startDate":"2016-06-30","endDate":"2019-07-12"},{"type":"appointment-of-board","details":"right-to-appoint-and-remove-directors","startDate":"2016-06-30","endDate":"2019-07-12"},{"type":"influence-or-control","details":"significant-influence-or-control","startDate":"2016-06-30","endDate":"2019-07-12"}],"source":{"type":["officialRegister"],"description":"GB Persons Of Significant Control Register","url":"http://download.companieshouse.gov.uk/en_pscdata.html","retrievedAt":"2019-07-25T09:59:00Z"}}'
json_line_18 = u'{"statementID":"openownership-register-4794195032142178395","statementType":"entityStatement","entityType":"registeredEntity","name":"THENOESIS LIMITED","incorporatedInJurisdiction":{"name":"United Kingdom of Great Britain and Northern Ireland","code":"GB"},"identifiers":[{"scheme":"GB-COH","schemeName":"Companies House","id":"08399361"},{"schemeName":"OpenCorporates","id":"https://opencorporates.com/companies/gb/08399361","uri":"https://opencorporates.com/companies/gb/08399361"},{"schemeName":"OpenOwnership Register","id":"http://register.openownership.org/entities/59b9194a67e4ebf340d2a458","uri":"http://register.openownership.org/entities/59b9194a67e4ebf340d2a458"}],"foundingDate":"2013-02-12","addresses":[{"type":"registered","address":"22 Chudleigh Way, Ruislip, Middlesex, HA4 8TP","country":"GB"}]}'
json_line_19 = u'{"statementID":"openownership-register-8670419579360932866","statementType":"personStatement","personType":"knownPerson","names":[{"type":"individual","fullName":"Vipin Madan"}],"identifiers":[{"schemeName":"GB Persons Of Significant Control Register","id":"/company/08399361/persons-with-significant-control/individual/oeC9rRd9uoUu9COlt88LFNLyKYk"},{"schemeName":"OpenOwnership Register","id":"http://register.openownership.org/entities/59b9194a67e4ebf340d2a461","uri":"http://register.openownership.org/entities/59b9194a67e4ebf340d2a461"}],"nationalities":[{"name":"United Kingdom of Great Britain and Northern Ireland","code":"GB"}],"birthDate":"1981-10-01","addresses":[{"address":"22, Chudleigh Way, Ruislip, Middlesex, HA4 8TP","country":"GB"}]}'
json_line_20 = u'{"statementID":"openownership-register-17922381292467623740","statementType":"ownershipOrControlStatement","statementDate":"2016-06-30","subject":{"describedByEntityStatement":"openownership-register-4794195032142178395"},"interestedParty":{"describedByPersonStatement":"openownership-register-8670419579360932866"},"interests":[{"type":"shareholding","details":"ownership-of-shares-50-to-75-percent","share":{"minimum":50,"maximum":75,"exclusiveMinimum":true,"exclusiveMaximum":true},"startDate":"2016-06-30"},{"type":"voting-rights","details":"voting-rights-50-to-75-percent","share":{"minimum":50,"maximum":75,"exclusiveMinimum":true,"exclusiveMaximum":true},"startDate":"2016-06-30"},{"type":"appointment-of-board","details":"right-to-appoint-and-remove-directors","startDate":"2016-06-30"}],"source":{"type":["officialRegister"],"description":"GB Persons Of Significant Control Register","url":"http://download.companieshouse.gov.uk/en_pscdata.html","retrievedAt":"2020-08-26T15:17:20Z"}}'
json_line_21 = u'{"statementID":"openownership-register-6793931124691591584","statementType":"personStatement","personType":"knownPerson","names":[{"type":"individual","fullName":"Renu Madan"}],"identifiers":[{"schemeName":"GB Persons Of Significant Control Register","id":"/company/08399361/persons-with-significant-control/individual/xFCSlf9czW03fks9IfWXKGwK3z8"},{"schemeName":"OpenOwnership Register","id":"http://register.openownership.org/entities/59b9d05f67e4ebf340d7324d","uri":"http://register.openownership.org/entities/59b9d05f67e4ebf340d7324d"}],"birthDate":"1985-06-01","addresses":[{"address":"22, Chudleigh Way, Ruislip, Middlesex, HA4 8TP"}]}'
json_line_22 = u'{"statementID":"openownership-register-273513485862738421","statementType":"ownershipOrControlStatement","statementDate":"2017-06-10","subject":{"describedByEntityStatement":"openownership-register-4794195032142178395"},"interestedParty":{"describedByPersonStatement":"openownership-register-6793931124691591584"},"interests":[{"type":"influence-or-control","details":"significant-influence-or-control","startDate":"2017-06-10"}],"source":{"type":["officialRegister"],"description":"GB Persons Of Significant Control Register","url":"http://download.companieshouse.gov.uk/en_pscdata.html","retrievedAt":"2019-07-08T10:37:11Z"}}'
json_line_23 = u'{"statementID":"openownership-register-5086228232034576807","statementType":"entityStatement","entityType":"registeredEntity","name":"G\u0026T BUILDER AND DECORATOR LTD","incorporatedInJurisdiction":{"name":"United Kingdom of Great Britain and Northern Ireland","code":"GB"},"identifiers":[{"scheme":"GB-COH","schemeName":"Companies House","id":"09103014"},{"schemeName":"OpenCorporates","id":"https://opencorporates.com/companies/gb/09103014","uri":"https://opencorporates.com/companies/gb/09103014"},{"schemeName":"OpenOwnership Register","id":"http://register.openownership.org/entities/59b9194a67e4ebf340d2a4ce","uri":"http://register.openownership.org/entities/59b9194a67e4ebf340d2a4ce"}],"foundingDate":"2014-06-25","addresses":[{"type":"registered","address":"7 Edgerton Road, Huddersfield, HD1 5RA","country":"GB"}]}'
json_line_24 = u'{"statementID":"openownership-register-12993071814636420030","statementType":"personStatement","personType":"knownPerson","names":[{"type":"individual","fullName":"Tadeusz Guzy"}],"identifiers":[{"schemeName":"GB Persons Of Significant Control Register","id":"/company/09103014/persons-with-significant-control/individual/qAqOsGFOgqZ5Q9qBiXkJSlvLYL4"},{"schemeName":"OpenOwnership Register","id":"http://register.openownership.org/entities/59b9194a67e4ebf340d2a4dc","uri":"http://register.openownership.org/entities/59b9194a67e4ebf340d2a4dc"}],"nationalities":[{"name":"Poland","code":"PL"}],"birthDate":"1958-05-01","addresses":[{"address":"7, Edgerton Road, Huddersfield, HD1 5RA","country":"GB"}]}'
json_line_25 = u'{"statementID":"openownership-register-16729651391258846630","statementType":"ownershipOrControlStatement","statementDate":"2016-06-29","subject":{"describedByEntityStatement":"openownership-register-5086228232034576807"},"interestedParty":{"describedByPersonStatement":"openownership-register-12993071814636420030"},"interests":[{"type":"shareholding","details":"ownership-of-shares-75-to-100-percent","share":{"minimum":75,"maximum":100,"exclusiveMinimum":false,"exclusiveMaximum":false},"startDate":"2016-06-29"},{"type":"voting-rights","details":"voting-rights-75-to-100-percent","share":{"minimum":75,"maximum":100,"exclusiveMinimum":false,"exclusiveMaximum":false},"startDate":"2016-06-29"},{"type":"appointment-of-board","details":"right-to-appoint-and-remove-directors-as-trust","startDate":"2016-06-29"},{"type":"appointment-of-board","details":"right-to-appoint-and-remove-directors-as-firm","startDate":"2016-06-29"},{"type":"influence-or-control","details":"significant-influence-or-control","startDate":"2016-06-29"},{"type":"influence-or-control","details":"significant-influence-or-control-as-trust","startDate":"2016-06-29"},{"type":"influence-or-control","details":"significant-influence-or-control-as-firm","startDate":"2016-06-29"}],"source":{"type":["officialRegister"],"description":"GB Persons Of Significant Control Register","url":"http://download.companieshouse.gov.uk/en_pscdata.html","retrievedAt":"2019-07-08T10:37:11Z"}}'
json_line_26 = u'{"statementID":"openownership-register-18372313541131017421","statementType":"entityStatement","entityType":"registeredEntity","name":"ALIGNED SOLUTIONS LIMITED","incorporatedInJurisdiction":{"name":"United Kingdom of Great Britain and Northern Ireland","code":"GB"},"identifiers":[{"scheme":"GB-COH","schemeName":"Companies House","id":"03328838"},{"schemeName":"OpenCorporates","id":"https://opencorporates.com/companies/gb/03328838","uri":"https://opencorporates.com/companies/gb/03328838"},{"schemeName":"OpenOwnership Register","id":"http://register.openownership.org/entities/59b9194a67e4ebf340d2a565","uri":"http://register.openownership.org/entities/59b9194a67e4ebf340d2a565"}],"foundingDate":"1997-03-06","dissolutionDate":"2018-03-06","addresses":[{"type":"registered","address":"Quadrus Centre Woodstock Way, Boldon Business Park, Boldon Colliery, Tyne And Wear, NE35 9PF","country":"GB"}]}'
json_line_27 = u'{"statementID":"openownership-register-2780379951819799961","statementType":"personStatement","personType":"knownPerson","names":[{"type":"individual","fullName":"David Ward"}],"identifiers":[{"schemeName":"GB Persons Of Significant Control Register","id":"/company/03328838/persons-with-significant-control/individual/P8DamFb7NgDYwqxblre98DIMcW0"},{"schemeName":"OpenOwnership Register","id":"http://register.openownership.org/entities/59b9194a67e4ebf340d2a574","uri":"http://register.openownership.org/entities/59b9194a67e4ebf340d2a574"}],"nationalities":[{"name":"United Kingdom of Great Britain and Northern Ireland","code":"GB"}],"birthDate":"1957-07-01","addresses":[{"address":"4, King Street, Pelaw, Gateshead, Tyne And Wear, NE10 0RD","country":"GB"}]}'
json_line_28 = u'{"statementID":"openownership-register-17339807231403546675","statementType":"ownershipOrControlStatement","statementDate":"2016-04-06","subject":{"describedByEntityStatement":"openownership-register-18372313541131017421"},"interestedParty":{"describedByPersonStatement":"openownership-register-2780379951819799961"},"interests":[{"type":"shareholding","details":"ownership-of-shares-75-to-100-percent","share":{"minimum":75,"maximum":100,"exclusiveMinimum":false,"exclusiveMaximum":false},"startDate":"2016-04-06"}],"source":{"type":["officialRegister"],"description":"GB Persons Of Significant Control Register","url":"http://download.companieshouse.gov.uk/en_pscdata.html","retrievedAt":"2019-07-08T10:37:11Z"}}'
json_line_29 = u'{"statementID":"openownership-register-11930345613553079310","statementType":"entityStatement","entityType":"registeredEntity","name":"CVF GROUP LIMITED","incorporatedInJurisdiction":{"name":"United Kingdom of Great Britain and Northern Ireland","code":"GB"},"identifiers":[{"scheme":"GB-COH","schemeName":"Companies House","id":"05565515"},{"schemeName":"OpenCorporates","id":"https://opencorporates.com/companies/gb/05565515","uri":"https://opencorporates.com/companies/gb/05565515"},{"schemeName":"OpenOwnership Register","id":"http://register.openownership.org/entities/59b9194a67e4ebf340d2a569","uri":"http://register.openownership.org/entities/59b9194a67e4ebf340d2a569"}],"foundingDate":"2005-09-16","addresses":[{"type":"registered","address":"11-31 School Lane, Rochdale, OL16 1QP","country":"GB"}]}'
json_line_30 = u'{"statementID":"openownership-register-2199726961708381448","statementType":"personStatement","personType":"knownPerson","names":[{"type":"individual","fullName":"David Castling Ward"}],"identifiers":[{"schemeName":"GB Persons Of Significant Control Register","id":"/company/05565515/persons-with-significant-control/individual/E6bj3QfO6aJWZzjUJzFUpqhuXPQ"},{"schemeName":"OpenOwnership Register","id":"http://register.openownership.org/entities/59b9194a67e4ebf340d2a57c","uri":"http://register.openownership.org/entities/59b9194a67e4ebf340d2a57c"}],"nationalities":[{"name":"United Kingdom of Great Britain and Northern Ireland","code":"GB"}],"birthDate":"1984-04-01","addresses":[{"address":"11-31, School Lane, Rochdale, OL16 1QP"}]}'
json_line_31 = u'{"statementID":"openownership-register-9118521193336573269","statementType":"ownershipOrControlStatement","statementDate":"2016-06-30","subject":{"describedByEntityStatement":"openownership-register-11930345613553079310"},"interestedParty":{"describedByPersonStatement":"openownership-register-2199726961708381448"},"interests":[{"type":"shareholding","details":"ownership-of-shares-75-to-100-percent","share":{"minimum":75,"maximum":100,"exclusiveMinimum":false,"exclusiveMaximum":false},"startDate":"2016-06-30"},{"type":"voting-rights","details":"voting-rights-75-to-100-percent","share":{"minimum":75,"maximum":100,"exclusiveMinimum":false,"exclusiveMaximum":false},"startDate":"2016-06-30"},{"type":"appointment-of-board","details":"right-to-appoint-and-remove-directors","startDate":"2016-06-30"},{"type":"influence-or-control","details":"significant-influence-or-control","startDate":"2016-06-30"}],"source":{"type":["officialRegister"],"description":"GB Persons Of Significant Control Register","url":"http://download.companieshouse.gov.uk/en_pscdata.html","retrievedAt":"2020-01-21T11:51:17Z"}}'
json_line_32 = u'{"statementID":"openownership-register-1617439713290008718","statementType":"entityStatement","entityType":"registeredEntity","name":"HIXONRUSSELL LTD.","incorporatedInJurisdiction":{"name":"United Kingdom of Great Britain and Northern Ireland","code":"GB"},"identifiers":[{"scheme":"GB-COH","schemeName":"Companies House","id":"07692524"},{"schemeName":"OpenCorporates","id":"https://opencorporates.com/companies/gb/07692524","uri":"https://opencorporates.com/companies/gb/07692524"},{"schemeName":"OpenOwnership Register","id":"http://register.openownership.org/entities/59b9194b67e4ebf340d2a679","uri":"http://register.openownership.org/entities/59b9194b67e4ebf340d2a679"}],"foundingDate":"2011-07-04","dissolutionDate":"2016-11-22","addresses":[{"type":"registered","address":"Aden House New Inn Road, Bartley, Southampton, Hants, SO40 2LR","country":"GB"}]}'
json_line_33 = u'{"statementID":"openownership-register-13787091824881447841","statementType":"personStatement","personType":"knownPerson","names":[{"type":"individual","fullName":"Alison Hixon"}],"identifiers":[{"schemeName":"GB Persons Of Significant Control Register","id":"/company/07692524/persons-with-significant-control/individual/8E0gN0FGol-wJMp3NNovK9dNGzE"},{"schemeName":"OpenOwnership Register","id":"http://register.openownership.org/entities/59b9194b67e4ebf340d2a680","uri":"http://register.openownership.org/entities/59b9194b67e4ebf340d2a680"}],"nationalities":[{"name":"United Kingdom of Great Britain and Northern Ireland","code":"GB"}],"birthDate":"1969-01-01","addresses":[{"address":"Aden House, New Inn Road, Bartley, Southampton, Hants, SO40 2LR"}]}'
json_line_34 = u'{"statementID":"openownership-register-4239324927539309956","statementType":"ownershipOrControlStatement","statementDate":"2016-06-30","subject":{"describedByEntityStatement":"openownership-register-1617439713290008718"},"interestedParty":{"describedByPersonStatement":"openownership-register-13787091824881447841"},"interests":[{"type":"influence-or-control","details":"significant-influence-or-control","startDate":"2016-06-30"}],"source":{"type":["officialRegister"],"description":"GB Persons Of Significant Control Register","url":"http://download.companieshouse.gov.uk/en_pscdata.html","retrievedAt":"2019-07-08T10:37:11Z"}}'
json_line_35 = u'{"statementID":"openownership-register-6739865532295039354","statementType":"entityStatement","entityType":"registeredEntity","name":"CSG CERAMIC TILING LIMITED","incorporatedInJurisdiction":{"name":"United Kingdom of Great Britain and Northern Ireland","code":"GB"},"identifiers":[{"scheme":"GB-COH","schemeName":"Companies House","id":"09665121"},{"schemeName":"OpenCorporates","id":"https://opencorporates.com/companies/gb/09665121","uri":"https://opencorporates.com/companies/gb/09665121"},{"schemeName":"OpenOwnership Register","id":"http://register.openownership.org/entities/59b9194b67e4ebf340d2a6a1","uri":"http://register.openownership.org/entities/59b9194b67e4ebf340d2a6a1"}],"foundingDate":"2015-07-01","addresses":[{"type":"registered","address":"17 Radiator Road, Great Cornard, Sudbury, CO10 0HX","country":"GB"}]}'
json_line_36 = u'{"statementID":"openownership-register-11619704309783115783","statementType":"personStatement","personType":"knownPerson","names":[{"type":"individual","fullName":"Chris Gillibrand"}],"identifiers":[{"schemeName":"GB Persons Of Significant Control Register","id":"/company/09665121/persons-with-significant-control/individual/IeDIPge4924lMrgLxo2QZb7CPzc"},{"schemeName":"OpenOwnership Register","id":"http://register.openownership.org/entities/59b9194b67e4ebf340d2a6a7","uri":"http://register.openownership.org/entities/59b9194b67e4ebf340d2a6a7"}],"nationalities":[{"name":"United Kingdom of Great Britain and Northern Ireland","code":"GB"}],"birthDate":"1973-03-01","addresses":[{"address":"17, Radiator Road, Great Cornard, Sudbury, CO10 0HX"}]}'
json_line_37 = u'{"statementID":"openownership-register-2393314999671423563","statementType":"ownershipOrControlStatement","statementDate":"2016-04-06","subject":{"describedByEntityStatement":"openownership-register-6739865532295039354"},"interestedParty":{"describedByPersonStatement":"openownership-register-11619704309783115783"},"interests":[{"type":"shareholding","details":"ownership-of-shares-75-to-100-percent","share":{"minimum":75,"maximum":100,"exclusiveMinimum":false,"exclusiveMaximum":false},"startDate":"2016-04-06"}],"source":{"type":["officialRegister"],"description":"GB Persons Of Significant Control Register","url":"http://download.companieshouse.gov.uk/en_pscdata.html","retrievedAt":"2020-04-24T09:24:16Z"}}'
json_line_38 = u'{"statementID":"openownership-register-2717498100609232791","statementType":"entityStatement","entityType":"registeredEntity","name":"RYEFIELD LIMITED","incorporatedInJurisdiction":{"name":"United Kingdom of Great Britain and Northern Ireland","code":"GB"},"identifiers":[{"scheme":"GB-COH","schemeName":"Companies House","id":"09086593"},{"schemeName":"OpenCorporates","id":"https://opencorporates.com/companies/gb/09086593","uri":"https://opencorporates.com/companies/gb/09086593"},{"schemeName":"OpenOwnership Register","id":"http://register.openownership.org/entities/59b9194b67e4ebf340d2a6c6","uri":"http://register.openownership.org/entities/59b9194b67e4ebf340d2a6c6"}],"foundingDate":"2014-06-16","addresses":[{"type":"registered","address":"Progress House, 404 Brighton Road, South Croydon, Surrey, CR2 6AN","country":"GB"}]}'
json_line_39 = u'{"statementID":"openownership-register-16015954137170957374","statementType":"personStatement","personType":"knownPerson","names":[{"type":"individual","fullName":"Leslie John Tasker"}],"identifiers":[{"schemeName":"GB Persons Of Significant Control Register","id":"/company/09086593/persons-with-significant-control/individual/DSPaXTV5wAW2xzBBgCbcUBr0wJ0"},{"schemeName":"OpenOwnership Register","id":"http://register.openownership.org/entities/59b9194b67e4ebf340d2a6db","uri":"http://register.openownership.org/entities/59b9194b67e4ebf340d2a6db"}],"nationalities":[{"name":"United Kingdom of Great Britain and Northern Ireland","code":"GB"}],"birthDate":"1961-12-01","addresses":[{"address":"Progress House, 404 Brighton Road, South Croydon, Surrey, CR2 6AN"}]}'
json_line_40 = u'{"statementID":"openownership-register-17784002375280431811","statementType":"ownershipOrControlStatement","statementDate":"2016-06-30","subject":{"describedByEntityStatement":"openownership-register-2717498100609232791"},"interestedParty":{"describedByPersonStatement":"openownership-register-16015954137170957374"},"interests":[{"type":"shareholding","details":"ownership-of-shares-75-to-100-percent","share":{"minimum":75,"maximum":100,"exclusiveMinimum":false,"exclusiveMaximum":false},"startDate":"2016-06-30"}],"source":{"type":["officialRegister"],"description":"GB Persons Of Significant Control Register","url":"http://download.companieshouse.gov.uk/en_pscdata.html","retrievedAt":"2019-07-08T10:37:11Z"}}'
json_line_41 = u'{"statementID":"openownership-register-16192320956788816388","statementType":"entityStatement","entityType":"registeredEntity","name":"TRELLIS LTD","incorporatedInJurisdiction":{"name":"United Kingdom of Great Britain and Northern Ireland","code":"GB"},"identifiers":[{"scheme":"GB-COH","schemeName":"Companies House","id":"07391326"},{"schemeName":"OpenCorporates","id":"https://opencorporates.com/companies/gb/07391326","uri":"https://opencorporates.com/companies/gb/07391326"},{"schemeName":"OpenOwnership Register","id":"http://register.openownership.org/entities/59b9194b67e4ebf340d2a84a","uri":"http://register.openownership.org/entities/59b9194b67e4ebf340d2a84a"}],"foundingDate":"2010-09-29","addresses":[{"type":"registered","address":"1 Watton Gardens, Bridport, DT6 3DG","country":"GB"}]}'
json_line_42 = u'{"statementID":"openownership-register-9331434081692566885","statementType":"personStatement","personType":"knownPerson","names":[{"type":"individual","fullName":"Timothy Howard Crabtree"}],"identifiers":[{"schemeName":"GB Persons Of Significant Control Register","id":"/company/07391326/persons-with-significant-control/individual/2KJivhO6sCNCWx1JK0SYGTl9Kvk"},{"schemeName":"OpenOwnership Register","id":"http://register.openownership.org/entities/59b9194b67e4ebf340d2a856","uri":"http://register.openownership.org/entities/59b9194b67e4ebf340d2a856"}],"nationalities":[{"name":"United Kingdom of Great Britain and Northern Ireland","code":"GB"}],"birthDate":"1962-10-01","addresses":[{"address":"1, Watton Gardens, Bridport, DT6 3DG","country":"GB"}]}'
json_line_43 = u'{"statementID":"openownership-register-2247033111811558176","statementType":"ownershipOrControlStatement","statementDate":"2016-06-30","subject":{"describedByEntityStatement":"openownership-register-16192320956788816388"},"interestedParty":{"describedByPersonStatement":"openownership-register-9331434081692566885"},"interests":[{"type":"shareholding","details":"ownership-of-shares-75-to-100-percent","share":{"minimum":75,"maximum":100,"exclusiveMinimum":false,"exclusiveMaximum":false},"startDate":"2016-06-30"}],"source":{"type":["officialRegister"],"description":"GB Persons Of Significant Control Register","url":"http://download.companieshouse.gov.uk/en_pscdata.html","retrievedAt":"2019-07-08T10:37:11Z"}}'
json_line_44 = u'{"statementID":"openownership-register-10261933605503846938","statementType":"entityStatement","entityType":"registeredEntity","name":"AFFINITY AVIATION GROUP LTD","incorporatedInJurisdiction":{"name":"United Kingdom of Great Britain and Northern Ireland","code":"GB"},"identifiers":[{"scheme":"GB-COH","schemeName":"Companies House","id":"09300323"},{"schemeName":"OpenCorporates","id":"https://opencorporates.com/companies/gb/09300323","uri":"https://opencorporates.com/companies/gb/09300323"},{"schemeName":"OpenOwnership Register","id":"http://register.openownership.org/entities/59b9194b67e4ebf340d2a8df","uri":"http://register.openownership.org/entities/59b9194b67e4ebf340d2a8df"}],"foundingDate":"2014-11-07","addresses":[{"type":"registered","address":"2nd Floor, Berkeley Square House, Berkeley Square, London, W1J 6BD","country":"GB"}]}'
json_line_45 = u'{"statementID":"openownership-register-10925933687062772116","statementType":"personStatement","personType":"knownPerson","names":[{"type":"individual","fullName":"Andrew David Hoy"}],"identifiers":[{"schemeName":"GB Persons Of Significant Control Register","id":"/company/09300323/persons-with-significant-control/individual/o7sWQeINDt7QvULUyVFos9xajLQ"},{"schemeName":"OpenOwnership Register","id":"http://register.openownership.org/entities/59b9194b67e4ebf340d2a8f3","uri":"http://register.openownership.org/entities/59b9194b67e4ebf340d2a8f3"}],"nationalities":[{"name":"United Kingdom of Great Britain and Northern Ireland","code":"GB"}],"birthDate":"1970-07-01","addresses":[{"address":"The Glebe, Loop Road, Keyston, Huntingdon, Cambridgeshire, PE28 0RE"}]}'
json_line_46 = u'{"statementID":"openownership-register-3988222398781891351","statementType":"ownershipOrControlStatement","statementDate":"2016-04-06","subject":{"describedByEntityStatement":"openownership-register-10261933605503846938"},"interestedParty":{"describedByPersonStatement":"openownership-register-10925933687062772116"},"interests":[{"type":"shareholding","details":"ownership-of-shares-75-to-100-percent","share":{"minimum":75,"maximum":100,"exclusiveMinimum":false,"exclusiveMaximum":false},"startDate":"2016-04-06"},{"type":"voting-rights","details":"voting-rights-75-to-100-percent","share":{"minimum":75,"maximum":100,"exclusiveMinimum":false,"exclusiveMaximum":false},"startDate":"2016-04-06"},{"type":"appointment-of-board","details":"right-to-appoint-and-remove-directors","startDate":"2016-04-06"},{"type":"influence-or-control","details":"significant-influence-or-control","startDate":"2016-04-06"}],"source":{"type":["officialRegister"],"description":"GB Persons Of Significant Control Register","url":"http://download.companieshouse.gov.uk/en_pscdata.html","retrievedAt":"2020-09-30T10:01:16Z"}}'
json_line_47 = u'{"statementID":"openownership-register-1760243829798270835","statementType":"entityStatement","entityType":"registeredEntity","name":"ARGENTUM FINANCIAL SERVICES LTD","incorporatedInJurisdiction":{"name":"United Kingdom of Great Britain and Northern Ireland","code":"GB"},"identifiers":[{"scheme":"GB-COH","schemeName":"Companies House","id":"09042837"},{"schemeName":"OpenCorporates","id":"https://opencorporates.com/companies/gb/09042837","uri":"https://opencorporates.com/companies/gb/09042837"},{"schemeName":"OpenOwnership Register","id":"http://register.openownership.org/entities/59b9194c67e4ebf340d2aa12","uri":"http://register.openownership.org/entities/59b9194c67e4ebf340d2aa12"}],"foundingDate":"2014-05-16","dissolutionDate":"2019-02-26","addresses":[{"type":"registered","address":"36 Chelmer Drive, South Ockendon, RM15 6EE","country":"GB"}]}'
json_line_48 = u'{"statementID":"openownership-register-1775950759050628389","statementType":"personStatement","personType":"knownPerson","names":[{"type":"individual","fullName":"Paljor Gurung"}],"identifiers":[{"schemeName":"GB Persons Of Significant Control Register","id":"/company/09042837/persons-with-significant-control/individual/r2fqQ2a4l6GbhnCgLf5SB8GfUlg"},{"schemeName":"OpenOwnership Register","id":"http://register.openownership.org/entities/59b9194c67e4ebf340d2aa26","uri":"http://register.openownership.org/entities/59b9194c67e4ebf340d2aa26"}],"birthDate":"1987-02-01","addresses":[{"address":"36, Chelmer Drive, South Ockendon, RM15 6EE"}]}'
json_line_49 = u'{"statementID":"openownership-register-11831699429343806005","statementType":"ownershipOrControlStatement","statementDate":"2016-06-30","subject":{"describedByEntityStatement":"openownership-register-1760243829798270835"},"interestedParty":{"describedByPersonStatement":"openownership-register-1775950759050628389"},"interests":[{"type":"shareholding","details":"ownership-of-shares-50-to-75-percent","share":{"minimum":50,"maximum":75,"exclusiveMinimum":true,"exclusiveMaximum":true},"startDate":"2016-06-30"},{"type":"shareholding","details":"ownership-of-shares-75-to-100-percent-as-trust","share":{"minimum":75,"maximum":100,"exclusiveMinimum":false,"exclusiveMaximum":false},"startDate":"2016-06-30"},{"type":"shareholding","details":"ownership-of-shares-50-to-75-percent-as-firm","share":{"minimum":50,"maximum":75,"exclusiveMinimum":true,"exclusiveMaximum":true},"startDate":"2016-06-30"},{"type":"voting-rights","details":"voting-rights-75-to-100-percent","share":{"minimum":75,"maximum":100,"exclusiveMinimum":false,"exclusiveMaximum":false},"startDate":"2016-06-30"},{"type":"voting-rights","details":"voting-rights-75-to-100-percent-as-trust","share":{"minimum":75,"maximum":100,"exclusiveMinimum":false,"exclusiveMaximum":false},"startDate":"2016-06-30"},{"type":"voting-rights","details":"voting-rights-75-to-100-percent-as-firm","share":{"minimum":75,"maximum":100,"exclusiveMinimum":false,"exclusiveMaximum":false},"startDate":"2016-06-30"},{"type":"appointment-of-board","details":"right-to-appoint-and-remove-directors","startDate":"2016-06-30"},{"type":"appointment-of-board","details":"right-to-appoint-and-remove-directors-as-trust","startDate":"2016-06-30"},{"type":"appointment-of-board","details":"right-to-appoint-and-remove-directors-as-firm","startDate":"2016-06-30"},{"type":"influence-or-control","details":"significant-influence-or-control","startDate":"2016-06-30"},{"type":"influence-or-control","details":"significant-influence-or-control-as-trust","startDate":"2016-06-30"},{"type":"influence-or-control","details":"significant-influence-or-control-as-firm","startDate":"2016-06-30"}],"source":{"type":["officialRegister"],"description":"GB Persons Of Significant Control Register","url":"http://download.companieshouse.gov.uk/en_pscdata.html","retrievedAt":"2019-07-08T10:37:11Z"}}'
json_line_50 = u'{"statementID":"openownership-register-11487464626996259406","statementType":"entityStatement","entityType":"registeredEntity","name":"BLACKROCK FINCO UK LTD","incorporatedInJurisdiction":{"name":"United Kingdom of Great Britain and Northern Ireland","code":"GB"},"identifiers":[{"schemeName":"GB Persons Of Significant Control Register","id":"/company/00951043/persons-with-significant-control/corporate-entity/uMkReuW1KppQmgrzeWV6vbGs5Qg"},{"schemeName":"GB Persons Of Significant Control Register - Registration numbers","id":"05853856"},{"scheme":"GB-COH","schemeName":"Companies House","id":"05853856"},{"schemeName":"OpenCorporates","id":"https://opencorporates.com/companies/gb/05853856","uri":"https://opencorporates.com/companies/gb/05853856"},{"schemeName":"OpenOwnership Register","id":"https://register.openownership.org/entities/59b92eab67e4ebf34028555b","uri":"https://register.openownership.org/entities/59b92eab67e4ebf34028555b"}],"foundingDate":"2006-06-21","addresses":[{"type":"registered","address":"12 Throgmorton Avenue, London, EC2N 2DL","country":"GB"}]}'
json_line_51 = u'{"statementID":"openownership-register-17960273783752226923","statementType":"entityStatement","entityType":"registeredEntity","name":"Blackrock, Inc.","incorporatedInJurisdiction":{"name":"United States of America","code":"US"},"identifiers":[{"schemeName":"GB Persons Of Significant Control Register","id":"/company/05853856/persons-with-significant-control/corporate-entity/9Bf8euMtqmNC-y5wl2uFWR496mA"},{"schemeName":"GB Persons Of Significant Control Register - Registration numbers","id":"32-0174431"},{"schemeName":"OpenOwnership Register","id":"https://register.openownership.org/entities/59b9d41b67e4ebf340e55156","uri":"https://register.openownership.org/entities/59b9d41b67e4ebf340e55156"}],"addresses":[{"type":"registered","address":"52, East, 52nd Street, New York, Ny 10055","country":"US"}]}'
json_line_52 = u'{"statementID":"openownership-register-17333232103253859659","statementType":"ownershipOrControlStatement","statementDate":"2016-04-06","subject":{"describedByEntityStatement":"openownership-register-11487464626996259406"},"interestedParty":{"describedByEntityStatement":"openownership-register-17960273783752226923"},"interests":[{"type":"shareholding","details":"ownership-of-shares-75-to-100-percent-as-firm","share":{"minimum":75,"maximum":100,"exclusiveMinimum":false,"exclusiveMaximum":false},"startDate":"2016-04-06"},{"type":"voting-rights","details":"voting-rights-75-to-100-percent-as-firm","share":{"minimum":75,"maximum":100,"exclusiveMinimum":false,"exclusiveMaximum":false},"startDate":"2016-04-06"},{"type":"appointment-of-board","details":"right-to-appoint-and-remove-directors-as-firm","startDate":"2016-04-06"}],"source":{"type":["officialRegister"],"description":"GB Persons Of Significant Control Register","url":"http://download.companieshouse.gov.uk/en_pscdata.html","retrievedAt":"2019-07-08T10:37:11Z"}}'
json_line_53 = u'{"statementID":"openownership-register-5305334539340172641","statementType":"ownershipOrControlStatement","subject":{"describedByEntityStatement":"openownership-register-17960273783752226923"},"interestedParty":{"unspecified":{"reason":"unknown","description":"Unknown person(s)"}},"interests":[]}'
```
create dynamic frame
```python
dyf_json = DynamicFrame.fromDF(df_json, glueContext, "dyf_json")
```
relationize
```python
dyf_relationize = dyf_json.relationalize("root", "/home/glue/GlueLocalOutput")
dyf_relationize.keys()
```
select table
```python
dyf_root = dyf_relationize.select('root')
dyf_root.toDF().show()
```
filter
```python
dyf_filter_root_ownership = Filter.apply(
frame = dyf_root, f = lambda x: x["statementType"] == 'ownershipOrControlStatement'
)

dyf_filter_root_ownership.toDF().select(['`interestedParty.unspecified.description`']).show()
```
filter again
```python
dyf_filter_root_no_ownership = Filter.apply(
frame = dyf_filter_root_ownership, f = lambda x: x["interestedParty.unspecified.description"] != None
)
dyf_filter_root_no_ownership.toDF().show()
```
write away
```python
glueContext.write_dynamic_frame.from_options( \
    frame = dyf_filter_root_no_ownership, \
    connection_options = {'path': ‘/home/glue/GlueLocalOutput/'}, \
    connection_type = 's3', \
    format = 'json')
```

## The ETL Workflow
create new stack in new python package. Add the following to the `__init__.py`
```python
class ETLStack(core.Stack):

    def __init__(self, scope, construct_id, raw_bucket: aws_s3.Bucket, script_bucket: aws_s3.Bucket):
        super().__init__(scope, construct_id, env=get_environment())
```
create glue database
```python
self.glue_database = aws_glue.Database(self, "HarvestDB",
                                  database_name="harvestdb",
                                  location_uri=raw_bucket.bucket_arn)
```
create service role method
```python
def create_glue_service_role(self) -> aws_iam.Role:
    glue_service_role = aws_iam.Role(self, "GlueServiceRole",
                                     assumed_by=aws_iam.ServicePrincipal("glue.amazonaws.com"))
    glue_service_role.add_to_policy(aws_iam.PolicyStatement(
        actions=[
            "s3:PutObject",
            "s3:DeleteObject",
            "s3:GetObject",
            "glue:*",
            "s3:GetBucketLocation",
            "s3:ListBucket",
            "s3:ListAllMyBuckets",
            "s3:GetBucketAcl",
            "ec2:DescribeVpcEndpoints",
            "ec2:DescribeRouteTables",
            "ec2:CreateNetworkInterface",
            "ec2:DeleteNetworkInterface",
            "ec2:DescribeNetworkInterfaces",
            "ec2:DescribeSecurityGroups",
            "ec2:DescribeSubnets",
            "ec2:DescribeVpcAttribute",
            "iam:ListRolePolicies",
            "iam:GetRole",
            "iam:GetRolePolicy",
            "cloudwatch:PutMetricData"
        ], effect=aws_iam.Effect.ALLOW, resources=["*"]))

    glue_service_role.add_to_policy(aws_iam.PolicyStatement(
        actions=[
            "s3:CreateBucket"
        ],
        effect=aws_iam.Effect.ALLOW,
        resources=[
            "arn:aws:s3:::aws-glue-*"
        ]))

    glue_service_role.add_to_policy(aws_iam.PolicyStatement(
        actions=[
            "s3:GetObject",
            "s3:PutObject",
            "s3:DeleteObject"
        ],
        effect=aws_iam.Effect.ALLOW,
        resources=[
            "arn:aws:s3:::aws-glue-*/*",
            "arn:aws:s3:::*/*aws-glue-*/*"
        ]))

    glue_service_role.add_to_policy(aws_iam.PolicyStatement(
        actions=[
            "s3:GetObject"
        ],
        effect=aws_iam.Effect.ALLOW,
        resources=[
            "arn:aws:s3:::crawler-public*",
            "arn:aws:s3:::aws-glue-*"
        ]))

    glue_service_role.add_to_policy(aws_iam.PolicyStatement(
        actions=[
            "logs:CreateLogGroup",
            "logs:CreateLogStream",
            "logs:PutLogEvents",
            "logs:AssociateKmsKey"
        ],
        effect=aws_iam.Effect.ALLOW,
        resources=[
            "arn:aws:logs:*:*:/aws-glue/*"
        ]))

    glue_service_role.add_to_policy(aws_iam.PolicyStatement(
        actions=[
            "ec2:CreateTags",
            "ec2:DeleteTags"
        ],
        effect=aws_iam.Effect.ALLOW,
        resources=[
            "arn:aws:ec2:*:*:network-interface/*",
            "arn:aws:ec2:*:*:security-group/*",
            "arn:aws:ec2:*:*:instance/*"
        ],
        conditions={"ForAllValues:StringEquals": {
            "aws:TagKeys": [
                "aws-glue-service-resource"
            ]
        }}))

    return glue_service_role
```
create workflow method
```python
def create_workflow(self,
                    database: aws_glue.Database,
                    script_bucket: aws_s3.Bucket,
                    raw_bucket: aws_s3.Bucket,
                    glue_service_role: aws_iam.Role):
```
create crawler
```python
crawler_target = CfnCrawler.S3TargetProperty(path=f"s3://{raw_bucket.bucket_name}/")
targets = CfnCrawler.TargetsProperty(s3_targets=[crawler_target])
raw_crawler = CfnCrawler(self, "S3GlueCrawler",
                         role=glue_service_role.role_arn,
                         targets=targets,
                         database_name=database.database_name,
                         name="RawCrawler"
                         )
```
create job
```python
command = CfnJob.JobCommandProperty(name="glueetl",
                                    python_version="3",
                                    script_location=f"s3://{script_bucket.bucket_name}/modifying_dataset.py")

missing_owner_job = CfnJob(self, "FindMissingOwnersGlueJob",
                           command=command,
                           role=glue_service_role.role_arn,
                           execution_property=CfnJob.ExecutionPropertyProperty(max_concurrent_runs=1),
                           max_retries=0,
                           name="FindMissingOwnersJob",
                           glue_version="2.0",
                           number_of_workers=2,
                           worker_type="Standard",
                           timeout=2,
                           )
```
create workflow
```python
harvest_workflow = aws_glue.CfnWorkflow(self, "HarvestWorkflow",
                                        default_run_properties=None,
                                        description="harvest workflow",
                                        name="HarvestWorkflow")
```
create crawler trigger
```python
class TriggerType:
    ON_DEMAND = "ON_DEMAND"
    CONDITIONAL = "CONDITIONAL"
    SCHEDULED = "SCHEDULED"

crawler_trigger = aws_glue.CfnTrigger(self, "CrawlerTrigger",
                                      actions=[
                                          aws_glue.CfnTrigger.ActionProperty(crawler_name=raw_crawler.name)
                                      ],
                                      type=TriggerType.ON_DEMAND,
                                      description="crawler trigger",
                                      name="CrawlerTrigger",
                                      workflow_name=harvest_workflow.name)

```
create job trigger
```python
job_trigger = aws_glue.CfnTrigger(self, "JobTrigger",
                                  actions=[
                                      aws_glue.CfnTrigger.ActionProperty(job_name=missing_owner_job.name)
                                  ],
                                  type=TriggerType.CONDITIONAL,
                                  description="job trigger",
                                  name="JobTrigger",
                                  predicate=aws_glue.CfnTrigger.PredicateProperty(conditions=[
                                      aws_glue.CfnTrigger.ConditionProperty(logical_operator="EQUALS",
                                                                            crawler_name=raw_crawler.name,
                                                                            crawl_state='SUCCEEDED')], ),
                                  start_on_creation=True,
                                  workflow_name=harvest_workflow.name)

```
get buckets
```shell
aws s3api list-buckets --query "Buckets[].Name" --profile harvest
```
change script with good paths
```python
dyf_json = glueContext.create_dynamic_frame_from_catalog(database="harvestdb", table_name="<bucket-name>")
```
upload script
```shell
aws s3 cp ./script.py s3://<scripts-bucket-name/script-name> --profile harvest
```
upload data
```shell
aws s3 cp ./harvest.json s3://<raw-bucket-name> --profile harvest
```
add stack to application
```python
etl = ETLStack(app, "etl-setup", base.raw_bucket, base.script_bucket)

```
synthesize stack
```shell
cdk synth --app 'python3 app.py' --profile harvest
```
deploy stack
```shell
cdk deploy etl-setup --profile harvest
```
run workflow
```shell
aws glue start-workflow-run --name HarvestWorkflow --profile harvest
```
see if table has been created
```shell
aws glue get-tables --database-name 'harvestdb' --profile harvest
```
see if data has been added to job location
```shell
aws s3 ls s3://<bucket-name>/ --profile harvest
```

### ETL Script example for upload.


```python
import sys

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import *
from pyspark.sql import Row
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

dyf_json = glueContext.create_dynamic_frame_from_catalog(database="harvestdb", table_name="base_setup_rawbucket0c3ee094_1bfdqv3sqfw1y")

dyf_relationize = dyf_json.relationalize("root", "s3://base-setup-modifiedbucket9b9e950b-14uguw1y5ohy")

dyf_selectFromCollection = SelectFromCollection.apply(dyf_relationize, 'root')

dyf_root = dyf_relationize.select('root')

dyf_filter_root_ownership = Filter.apply(frame = dyf_root, f = lambda x: x["statementType"] == 'ownershipOrControlStatement')

dyf_filter_root_no_ownership = Filter.apply(frame = dyf_filter_root_ownership, f = lambda x: x["interestedParty.unspecified.description"] != None)

glueContext.write_dynamic_frame.from_options(\
    frame = dyf_filter_root_no_ownership,\
    connection_options={'path': 's3://base-setup-modifiedbucket9b9e950b-14uguw1y5ohy'},\
    connection_type='s3',\
    format='parquet')



```
