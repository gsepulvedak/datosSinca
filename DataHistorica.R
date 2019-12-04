library(tidyr)
library(purrr)
library(lubridate)
library(plyr)
library(dplyr)
library(RODBC)

#Obtener diccionario estaciones de BD
cn <- odbcDriverConnect('driver={SQL Server};server=nborchers\\sqlserver_nba;database=Monitoreo_MMA;uid=prueba;pwd=sma2018..')
Estacion <- sqlQuery(cn, "select * from Estacion")
close(cn)

source("~/R_wd/DatosSinca/R/FunDataHist.R")

#Obtener MP2,5
url <- makeUrlCal(region = "RM", param = "PM25")
sets <- getCalData() #Obtención de datos en una lista
MP25 <- sets %>% Reduce(function(a,b) dplyr::union(a,b),. ); rm(sets) #Generación de dataframe
MP25 <- cleanCalData(data = MP25, vbl = "MP25") #Limpieza y ajustes de formato
MP25 <- select(MP25, Fecha, ConMP25, TipoDato, Ano, Mes, Dia, Hora, Region, Comuna, Monitor, Codigo) #Selección de tabla final

#Guardar registros en BD
cn <- odbcReConnect(cn)
sqlSave(cn, MP25, tablename = "MP25", rownames = FALSE, varTypes = c(Fecha = "datetime"))
close(cn); rm(MP25, url)

#Obtener MP10
url <- makeUrlCal(region = "RM", param = "PM10")
sets <- getCalData() #Obtención de datos en una lista
MP10 <- sets %>% Reduce(function(a,b) dplyr::union(a,b),. ); rm(sets) #Generación de dataframe
MP10 <- cleanCalData(data = MP10, vbl = "MP10") #Limpieza y ajustes de formato
MP10 <- select(MP10, Fecha, ConMP10, TipoDato, Ano, Mes, Dia, Hora, Region, Comuna, Monitor, Codigo) #Selección de tabla final

#Guardar registros en BD
cn <- odbcReConnect(cn)
sqlSave(cn, MP10, tablename = "MP10", rownames = FALSE, varTypes = c(Fecha = "datetime"))
close(cn); rm(MP10, url)

#Obtener SO2
url <- makeUrlCal(region = "RM", param = "0001")
sets <- getCalData() #Obtención de datos en una lista
SO2 <- sets %>% Reduce(function(a,b) dplyr::union(a,b),. ); rm(sets) #Generación de dataframe
SO2 <- cleanCalData(data = SO2, vbl = "SO2") #Limpieza y ajustes de formato
SO2 <- select(SO2, Fecha, ConSO2, TipoDato, Ano, Mes, Dia, Hora, Region, Comuna, Monitor, Codigo) #Selección de tabla final

#Guardar registros en BD
cn <- odbcReConnect(cn)
sqlSave(cn, SO2, tablename = "SO2", rownames = FALSE, varTypes = c(Fecha = "datetime"))
close(cn); rm(SO2, url)

#Obtener NO2
url <- makeUrlCal(region = "RM", param = "0003")
sets <- getCalData() #Obtención de datos en una lista
NO2 <- sets %>% Reduce(function(a,b) dplyr::union(a,b),. ); rm(sets) #Generación de dataframe
NO2 <- cleanCalData(data = NO2, vbl = "NO2") #Limpieza y ajustes de formato
NO2 <- select(NO2, Fecha, ConNO2, TipoDato, Ano, Mes, Dia, Hora, Region, Comuna, Monitor, Codigo) #Selección de tabla final

#Guardar registros en BD
cn <- odbcReConnect(cn)
sqlSave(cn, NO2, tablename = "NO2", rownames = FALSE, varTypes = c(Fecha = "datetime"))
close(cn); rm(NO2, url)

#Obtener NOX
url <- makeUrlCal(region = "RM", param = "0NOX")
sets <- getCalData() #Obtención de datos en una lista
NOX <- sets %>% Reduce(function(a,b) dplyr::union(a,b),. ); rm(sets) #Generación de dataframe
NOX <- cleanCalData(data = NOX, vbl = "NOX") #Limpieza y ajustes de formato
NOX <- select(NOX, Fecha, ConNOX, TipoDato, Ano, Mes, Dia, Hora, Region, Comuna, Monitor, Codigo) #Selección de tabla final

#Guardar registros en BD
cn <- odbcReConnect(cn)
sqlSave(cn, NOX, tablename = "NOX", rownames = FALSE, varTypes = c(Fecha = "datetime"))
close(cn); rm(NOX, url)

#Obtener NO
url <- makeUrlCal(region = "RM", param = "0002")
sets <- getCalData() #Obtención de datos en una lista
NO <- sets %>% Reduce(function(a,b) dplyr::union(a,b),. ); rm(sets) #Generación de dataframe
NO <- cleanCalData(data = NO, vbl = "NO") #Limpieza y ajustes de formato
NO <- select(NO, Fecha, ConNO, TipoDato, Ano, Mes, Dia, Hora, Region, Comuna, Monitor, Codigo) #Selección de tabla final

#Guardar registros en BD
cn <- odbcReConnect(cn)
sqlSave(cn, NO, tablename = "NO", rownames = FALSE, varTypes = c(Fecha = "datetime"))
close(cn); rm(NO, url)

#Obtener CO
url <- makeUrlCal(region = "RM", param = "0004")
sets <- getCalData() #Obtención de datos en una lista
CO <- sets %>% Reduce(function(a,b) dplyr::union(a,b),. ); rm(sets) #Generación de dataframe
CO <- cleanCalData(data = CO, vbl = "CO") #Limpieza y ajustes de formato
CO <- select(CO, Fecha, ConCO, TipoDato, Ano, Mes, Dia, Hora, Region, Comuna, Monitor, Codigo) #Selección de tabla final

#Guardar registros en BD
cn <- odbcReConnect(cn)
sqlSave(cn, CO, tablename = "CO", rownames = FALSE, varTypes = c(Fecha = "datetime"))
close(cn); rm(CO, url)

#Obtener O3
url <- makeUrlCal(region = "RM", param = "0008")
sets <- getCalData() #Obtención de datos en una lista
O3 <- sets %>% Reduce(function(a,b) dplyr::union(a,b),. ); rm(sets) #Generación de dataframe
O3 <- cleanCalData(data = O3, vbl = "O3") #Limpieza y ajustes de formato
O3 <- select(O3, Fecha, ConO3, TipoDato, Ano, Mes, Dia, Hora, Region, Comuna, Monitor, Codigo) #Selección de tabla final

#Guardar registros en BD
cn <- odbcReConnect(cn)
sqlSave(cn, O3, tablename = "O3", rownames = FALSE, varTypes = c(Fecha = "datetime"))
close(cn); rm(O3, url)

#Obtener CH4
url <- makeUrlCal(region = "RM", param = "0CH4")
sets <- getCalData() #Obtención de datos en una lista
CH4 <- sets %>% Reduce(function(a,b) dplyr::union(a,b),. ); rm(sets) #Generación de dataframe
CH4 <- cleanCalData(data = CH4, vbl = "CH4") #Limpieza y ajustes de formato
CH4 <- select(CH4, Fecha, ConCH4, TipoDato, Ano, Mes, Dia, Hora, Region, Comuna, Monitor, Codigo) #Selección de tabla final

#Guardar registros en BD
cn <- odbcReConnect(cn)
sqlSave(cn, CH4, tablename = "CH4", rownames = FALSE, varTypes = c(Fecha = "datetime"))
close(cn); rm(CH4, url)

#Obtener HCNM
url <- makeUrlCal(region = "RM", param = "NMHC")
sets <- getCalData() #Obtención de datos en una lista
HCNM <- sets %>% Reduce(function(a,b) dplyr::union(a,b),. ); rm(sets) #Generación de dataframe
HCNM <- cleanCalData(data = HCNM, vbl = "HCNM") #Limpieza y ajustes de formato
HCNM <- select(HCNM, Fecha, ConHCNM, TipoDato, Ano, Mes, Dia, Hora, Region, Comuna, Monitor, Codigo) #Selección de tabla final

#Guardar registros en BD
cn <- odbcReConnect(cn)
sqlSave(cn, HCNM, tablename = "HCNM", rownames = FALSE, varTypes = c(Fecha = "datetime"))
close(cn); rm(HCNM, url)

#Obtener humedad relativa
url <- makeUrlMet(region = "RM", param = "RHUM")
sets <-getMetData()
HR <- sets %>% Reduce(function(a,b) dplyr::union(a,b),. ); rm(sets) #Generación de dataframe
HR <- cleanMetData(data = HR, var = "HR") #Limpieza y ajustes de formato
HR <- select(HR, Fecha, HR, Ano, Mes, Dia, Hora, Region, Comuna, Monitor, Codigo)

#Guardar registros en BD
cn <- odbcReConnect(cn)
sqlSave(cn, HR, tablename = "HR", rownames = FALSE, varTypes = c(Fecha = "datetime"))
close(cn); rm(HR, url)

#Obtener temperatura
url <- makeUrlMet(region = "RM", param = "TEMP")
sets <-getMetData()
TEMP <- sets %>% Reduce(function(a,b) dplyr::union(a,b),. ); rm(sets) #Generación de dataframe
TEMP <- cleanMetData(data = TEMP, var = "TEMP") #Limpieza y ajustes de formato
TEMP <- select(TEMP, Fecha, TEMP, Ano, Mes, Dia, Hora, Region, Comuna, Monitor, Codigo)

#Guardar registros en BD
cn <- odbcReConnect(cn)
sqlSave(cn, TEMP, tablename = "Temperatura", rownames = FALSE, varTypes = c(Fecha = "datetime"))
close(cn); rm(TEMP, url)

#Obtener dirección viento
url <- makeUrlMet(region = "RM", param = "WDIR")
sets <-getMetData()
WDIR <- sets %>% Reduce(function(a,b) dplyr::union(a,b),. ); rm(sets) #Generación de dataframe
WDIR <- cleanMetData(data = WDIR, var = "WDIR") #Limpieza y ajustes de formato
WDIR <- select(WDIR, Fecha, WDIR, Ano, Mes, Dia, Hora, Region, Comuna, Monitor, Codigo)

#Guardar registros en BD
cn <- odbcReConnect(cn)
sqlSave(cn, WDIR, tablename = "DirViento", rownames = FALSE, varTypes = c(Fecha = "datetime"))
close(cn); rm(WDIR, url)

#Obtener velocidad viento
url <- makeUrlMet(region = "RM", param = "WSPD")
sets <-getMetData()
WSPD <- sets %>% Reduce(function(a,b) dplyr::union(a,b),. ); rm(sets) #Generación de dataframe
WSPD <- cleanMetData(data = WSPD, var = "WSPD") #Limpieza y ajustes de formato
WSPD <- select(WSPD, Fecha, WSPD, Ano, Mes, Dia, Hora, Region, Comuna, Monitor, Codigo)

#Guardar registros en BD
cn <- odbcReConnect(cn)
sqlSave(cn, WSPD, tablename = "VelViento", rownames = FALSE, varTypes = c(Fecha = "datetime"))
close(cn); rm(cn, WSPD, url)