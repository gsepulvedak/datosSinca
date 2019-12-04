library(tidyverse)
library(lubridate)
library(RODBC)
library(stringr)

#Obtener diccionario estaciones de BD
cn <- odbcDriverConnect('driver={SQL Server};server=nborchers\\sqlserver_nba;database=Monitoreo_MMA;uid=prueba;pwd=sma2018..')
Estacion <- sqlQuery(cn, "select * from Estacion")
close(cn)

source("~/R_wd/DatosSinca/R/FunDataNueva.R")

#Obtener MP2,5
updt <- getLastDate("MP25")
url <- makeUrlCal(region = "RM", param = "MP25", df = updt)
sets <- getCalData() #Obtención de datos en una lista
if(length(sets) > 0){
  MP25 <- sets %>% Reduce(function(a,b) dplyr::union(a,b),. ); rm(sets) #Generación de dataframe
  MP25 <- cleanCalData(data = MP25, vbl = "MP25") #Limpieza y ajustes de formato
  MP25 <- select(MP25, Fecha, ConMP25, TipoDato, Ano, Mes, Dia, Hora, Region, Comuna, Monitor, Codigo) #Selección de tabla final
  
  #Guardar registros en BD
  cn <- odbcReConnect(cn)
  sqlSave(cn, MP25, tablename = "MP25", rownames = FALSE, colnames = FALSE, varTypes = c(Fecha = "datetime"), append = TRUE)
  close(cn); rm(MP25, url, updt)
} else {
  rm(sets, updt, url)
}

#Obtener MP10
updt <- getLastDate("MP10")
url <- makeUrlCal(region = "RM", param = "MP10", df = updt)
sets <- getCalData() #Obtención de datos en una lista
if(length(sets) > 0){
  MP10 <- sets %>% Reduce(function(a,b) dplyr::union(a,b),. ); rm(sets) #Generación de dataframe
  MP10 <- cleanCalData(data = MP10, vbl = "MP10") #Limpieza y ajustes de formato
  MP10 <- select(MP10, Fecha, ConMP10, TipoDato, Ano, Mes, Dia, Hora, Region, Comuna, Monitor, Codigo) #Selección de tabla final
  
  #Guardar registros en BD
  cn <- odbcReConnect(cn)
  sqlSave(cn, MP10, tablename = "MP10", rownames = FALSE, colnames = FALSE, varTypes = c(Fecha = "datetime"), append = TRUE)
  close(cn); rm(MP10, url, updt)
} else {
  rm(sets, updt, url)
}

#Obtener SO2
updt <- getLastDate("SO2")
url <- makeUrlCal(region = "RM", param = "SO2", df = updt)
sets <- getCalData() #Obtención de datos en una lista
if(length(sets) > 0){
  SO2 <- sets %>% Reduce(function(a,b) dplyr::union(a,b),. ); rm(sets) #Generación de dataframe
  SO2 <- cleanCalData(data = SO2, vbl = "SO2") #Limpieza y ajustes de formato
  SO2 <- select(SO2, Fecha, ConSO2, TipoDato, Ano, Mes, Dia, Hora, Region, Comuna, Monitor, Codigo) #Selección de tabla final
  
  #Guardar registros en BD
  cn <- odbcReConnect(cn)
  sqlSave(cn, SO2, tablename = "SO2", rownames = FALSE, colnames = FALSE, varTypes = c(Fecha = "datetime"), append = TRUE)
  close(cn); rm(SO2, url, updt)
} else {
  rm(sets, updt, url)
}

#Obtener NO2
updt <- getLastDate("NO2")
url <- makeUrlCal(region = "RM", param = "NO2", df = updt)
sets <- getCalData() #Obtención de datos en una lista
if(length(sets) > 0){
  NO2 <- sets %>% Reduce(function(a,b) dplyr::union(a,b),. ); rm(sets) #Generación de dataframe
  NO2 <- cleanCalData(data = NO2, vbl = "NO2") #Limpieza y ajustes de formato
  NO2 <- select(NO2, Fecha, ConNO2, TipoDato, Ano, Mes, Dia, Hora, Region, Comuna, Monitor, Codigo) #Selección de tabla final
  
  #Guardar registros en BD
  cn <- odbcReConnect(cn)
  sqlSave(cn, NO2, tablename = "NO2", rownames = FALSE, colnames = FALSE, varTypes = c(Fecha = "datetime"), append = TRUE)
  close(cn); rm(NO2, url, updt)
} else {
  rm(sets, updt, url)
}

#Obtener NOX
updt <- getLastDate("NOX")
url <- makeUrlCal(region = "RM", param = "NOX", df = updt)
sets <- getCalData() #Obtención de datos en una lista
if(length(sets) > 0){
  NOX <- sets %>% Reduce(function(a,b) dplyr::union(a,b),. ); rm(sets) #Generación de dataframe
  NOX <- cleanCalData(data = NOX, vbl = "NOX") #Limpieza y ajustes de formato
  NOX <- select(NOX, Fecha, ConNOX, TipoDato, Ano, Mes, Dia, Hora, Region, Comuna, Monitor, Codigo) #Selección de tabla final
  
  #Guardar registros en BD
  cn <- odbcReConnect(cn)
  sqlSave(cn, NOX, tablename = "NOX", rownames = FALSE, colnames = FALSE, varTypes = c(Fecha = "datetime"), append = TRUE)
  close(cn); rm(NOX, url, updt)
} else {
  rm(sets, updt, url)
}

#Obtener NO
updt <- getLastDate("NO")
url <- makeUrlCal(region = "RM", param = "NO", df = updt)
sets <- getCalData() #Obtención de datos en una lista
if(length(sets) > 0){
  NO <- sets %>% Reduce(function(a,b) dplyr::union(a,b),. ); rm(sets) #Generación de dataframe
  NO <- cleanCalData(data = NO, vbl = "NO") #Limpieza y ajustes de formato
  NO <- select(NO, Fecha, ConNO, TipoDato, Ano, Mes, Dia, Hora, Region, Comuna, Monitor, Codigo) #Selección de tabla final
  
  #Guardar registros en BD
  cn <- odbcReConnect(cn)
  sqlSave(cn, NO, tablename = "NO", rownames = FALSE, colnames = FALSE, varTypes = c(Fecha = "datetime"), append = TRUE)
  close(cn); rm(NO, url, updt)
} else {
  rm(sets, updt, url)
}

#Obtener CO
updt <- getLastDate("CO")
url <- makeUrlCal(region = "RM", param = "CO", df = updt)
sets <- getCalData() #Obtención de datos en una lista
if(length(sets) > 0){
  CO <- sets %>% Reduce(function(a,b) dplyr::union(a,b),. ); rm(sets) #Generación de dataframe
  CO <- cleanCalData(data = CO, vbl = "CO") #Limpieza y ajustes de formato
  CO <- select(CO, Fecha, ConCO, TipoDato, Ano, Mes, Dia, Hora, Region, Comuna, Monitor, Codigo) #Selección de tabla final
  
  #Guardar registros en BD
  cn <- odbcReConnect(cn)
  sqlSave(cn, CO, tablename = "CO", rownames = FALSE, colnames = FALSE, varTypes = c(Fecha = "datetime"), append = TRUE)
  close(cn); rm(CO, url, updt)
} else {
  rm(sets, updt, url)
}

#Obtener O3
updt <- getLastDate("O3")
url <- makeUrlCal(region = "RM", param = "O3", df = updt)
sets <- getCalData() #Obtención de datos en una lista
if(length(sets) > 0){
  O3 <- sets %>% Reduce(function(a,b) dplyr::union(a,b),. ); rm(sets) #Generación de dataframe
  O3 <- cleanCalData(data = O3, vbl = "O3") #Limpieza y ajustes de formato
  O3 <- select(O3, Fecha, ConO3, TipoDato, Ano, Mes, Dia, Hora, Region, Comuna, Monitor, Codigo) #Selección de tabla final
  
  #Guardar registros en BD
  cn <- odbcReConnect(cn)
  sqlSave(cn, O3, tablename = "O3", rownames = FALSE, colnames = FALSE, varTypes = c(Fecha = "datetime"), append = TRUE)
  close(cn); rm(O3, url, updt)
} else {
  rm(sets, updt, url)
}

#Obtener CH4
updt <- getLastDate("CH4")
url <- makeUrlCal(region = "RM", param = "CH4", df = updt)
sets <- getCalData() #Obtención de datos en una lista
if(length(sets) > 0){
  CH4 <- sets %>% Reduce(function(a,b) dplyr::union(a,b),. ); rm(sets) #Generación de dataframe
  CH4 <- cleanCalData(data = CH4, vbl = "CH4") #Limpieza y ajustes de formato
  CH4 <- select(CH4, Fecha, ConCH4, TipoDato, Ano, Mes, Dia, Hora, Region, Comuna, Monitor, Codigo) #Selección de tabla final
  
  #Guardar registros en BD
  cn <- odbcReConnect(cn)
  sqlSave(cn, CH4, tablename = "CH4", rownames = FALSE, colnames = FALSE, varTypes = c(Fecha = "datetime"), append = TRUE)
  close(cn); rm(CH4, url, updt)
} else {
  rm(sets, updt, url)
}

#Obtener HCNM
updt <- getLastDate("HCNM")
url <- makeUrlCal(region = "RM", param = "HCNM", df = updt)
sets <- getCalData() #Obtención de datos en una lista
if(length(sets) > 0){
  HCNM <- sets %>% Reduce(function(a,b) dplyr::union(a,b),. ); rm(sets) #Generación de dataframe
  HCNM <- cleanCalData(data = HCNM, vbl = "HCNM") #Limpieza y ajustes de formato
  HCNM <- select(HCNM, Fecha, ConHCNM, TipoDato, Ano, Mes, Dia, Hora, Region, Comuna, Monitor, Codigo) #Selección de tabla final
  
  #Guardar registros en BD
  cn <- odbcReConnect(cn)
  sqlSave(cn, HCNM, tablename = "HCNM", rownames = FALSE, colnames = FALSE, varTypes = c(Fecha = "datetime"), append = TRUE)
  close(cn); rm(HCNM, url, updt)
} else {
  rm(sets, updt, url)
}

#Obtener humedad relativa
updt <- getLastDate("HR")
url <- makeUrlMet(region = "RM", param = "HR", df = updt)
sets <-getMetData()
if(length(sets) > 0){
  HR <- sets %>% Reduce(function(a,b) dplyr::union(a,b),. ); rm(sets) #Generación de dataframe
  HR <- cleanMetData(data = HR, var = "HR") #Limpieza y ajustes de formato
  HR <- select(HR, Fecha, HR, Ano, Mes, Dia, Hora, Region, Comuna, Monitor, Codigo)
  
  #Guardar registros en BD
  cn <- odbcReConnect(cn)
  sqlSave(cn, HR, tablename = "HR", rownames = FALSE, colnames = FALSE, varTypes = c(Fecha = "datetime"), append = TRUE)
  close(cn); rm(HR, url, updt)
} else {
  rm(sets, updt, url)
}

#Obtener temperatura
updt <- getLastDate("Temperatura")
url <- makeUrlMet(region = "RM", param = "Temperatura", df = updt)
sets <-getMetData()
if(length(sets) > 0){
  TEMP <- sets %>% Reduce(function(a,b) dplyr::union(a,b),. ); rm(sets) #Generación de dataframe
  TEMP <- cleanMetData(data = TEMP, var = "TEMP") #Limpieza y ajustes de formato
  TEMP <- select(TEMP, Fecha, TEMP, Ano, Mes, Dia, Hora, Region, Comuna, Monitor, Codigo)
  
  #Guardar registros en BD
  cn <- odbcReConnect(cn)
  sqlSave(cn, TEMP, tablename = "Temperatura", rownames = FALSE, colnames = FALSE, varTypes = c(Fecha = "datetime"), append = TRUE)
  close(cn); rm(TEMP, url, updt)
} else {
  rm(sets, updt, url)
}

#Obtener dirección viento
updt <- getLastDate("DirViento")
url <- makeUrlMet(region = "RM", param = "DirViento", df = updt)
sets <-getMetData()
if(length(sets) > 0){
  WDIR <- sets %>% Reduce(function(a,b) dplyr::union(a,b),. ); rm(sets) #Generación de dataframe
  WDIR <- cleanMetData(data = WDIR, var = "WDIR") #Limpieza y ajustes de formato
  WDIR <- select(WDIR, Fecha, WDIR, Ano, Mes, Dia, Hora, Region, Comuna, Monitor, Codigo)
  
  #Guardar registros en BD
  cn <- odbcReConnect(cn)
  sqlSave(cn, WDIR, tablename = "DirViento", rownames = FALSE, colnames = FALSE, varTypes = c(Fecha = "datetime"), append = TRUE)
  close(cn); rm(WDIR, url, updt)
} else {
  rm(sets, updt, url)
}

#Obtener velocidad viento
updt <- getLastDate("VelViento")
url <- makeUrlMet(region = "RM", param = "VelViento", df = updt)
sets <-getMetData()
if(length(sets) > 0){
  WSPD <- sets %>% Reduce(function(a,b) dplyr::union(a,b),. ); rm(sets) #Generación de dataframe
  WSPD <- cleanMetData(data = WSPD, var = "WSPD") #Limpieza y ajustes de formato
  WSPD <- select(WSPD, Fecha, WSPD, Ano, Mes, Dia, Hora, Region, Comuna, Monitor, Codigo)
  
  #Guardar registros en BD
  cn <- odbcReConnect(cn)
  sqlSave(cn, WSPD, tablename = "VelViento", rownames = FALSE, colnames = FALSE, varTypes = c(Fecha = "datetime"), append = TRUE)
  close(cn); rm(cn, WSPD, url, updt)
} else {
  rm(sets, updt, url, cn)
}
