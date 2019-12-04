#obtener última fecha para parámetro indicado por estación
getLastDate <- function(param){
  query <- paste("select Monitor, max(Fecha) as maxFecha from", param, "group by Monitor")
  cn <- odbcDriverConnect('driver={SQL Server};server=nborchers\\sqlserver_nba;dataEstacion=Nombreeo_MMA;uid=prueba;pwd=sma2018..')
  ultFecha <- sqlQuery(cn, query = query)
  close(cn); rm(cn)
  ultFecha <- mutate(ultFecha, ultFechaFmt = as.character(format(ultFecha$maxFecha, "%y%m%d"))
  )
  ultFecha <- dplyr::left_join(Estacion, ultFecha, by = c("Nombre" = "Monitor")) %>% select(Nombre, maxFecha, ultFechaFmt)
  return(ultFecha)
}

#Generar URL de descarga de csv para datos de calidad
makeUrlCal <- function(region, param, df){
  if(param == "MP25") param <- "PM25"
  if(param == "MP10") param <- "PM10"
  if(param == "SO2") param <- "0001"
  if(param == "NO2") param <- "0003"
  if(param == "NOX") param <- "0NOX"
  if(param == "NO") param <- "0002"
  if(param == "CO") param <- "0004"
  if(param == "O3") param <- "0008"
  if(param == "CH4") param <- "0CH4"
  if(param == "HCNM") param <- "NMHC"
  ayer <- format(today(tzone = "America/Santiago")-1, "%y%m%d")
  url1 <-paste0("http://sinca.mma.gob.cl/cgi-bin/APUB-MMA/apub.tsindico2.cgi?outtype=xcl&macro=./",region,"/")
  url2 <- paste0("/Cal/",param,"//",param,".horario.horario.ic")
  url <- map2(Estacion$Carpeta, df$ultFechaFmt, ~paste0(url1, .x, url2, "&from=", .y, "&to=", ayer))
  url <- lapply(url, str_replace_all, "NA", "")
}

#Crear URL para descarga de variables meteorológicas
makeUrlMet <- function(region, param, df){
  if(param == "HR") param <- "RHUM"
  if(param == "Temperatura") param <- "TEMP"
  if(param == "DirViento") param <- "WDIR"
  if(param == "VelViento") param <- "WSPD"
  ayer <- format(today(tzone = "America/Santiago")-1, "%y%m%d")
  url1 <-paste0("http://sinca.mma.gob.cl/cgi-bin/APUB-MMA/apub.tsindico2.cgi?outtype=xcl&macro=./",region,"/")
  url2 <- ifelse(param == "WDIR", paste0("/Met/",param,"//horario_000_spec.ic"), paste0("/Met/",param,"//horario_000.ic"))
  url <- map2(Estacion$Carpeta, df$ultFechaFmt, ~paste0(url1, .x, url2, "&from=", .y, "&to=", ayer))
  url <- lapply(url, str_replace_all, "NA", "")
}

#Obtener set de datos de calidad
getCalData <- function(){
  sets <- lapply(url, read.csv2, header = TRUE, 
                 colClasses = c("integer", "integer",rep("numeric",3), "NULL"), dec = ",")
  names(sets) <- Estacion$Nombre
  sets <- lapply(sets, function(x) gather(x, TipoDato, vbl, 3:5, na.rm = TRUE, factor_key = FALSE))
  sets <- lapply(sets, function(x) {if(nrow(x) == 0) x <- NULL; x})
  sets <- compact(sets)
  sets <- lapply(sets, function(x) mutate(x, Fechatmp = paste0(sprintf("%06.0f", x$FECHA..YYMMDD.), sprintf("%04.0f", x$HORA..HHMM.))))
  sets <- lapply(sets, function(x) mutate(x, Fecha = ymd_hm(x$Fechatmp, tz = "America/Santiago")))
  sets <- lapply(sets, function(x) mutate(x, Ano = year(x$Fecha), Mes = month(x$Fecha), Dia = day(x$Fecha), Hora = hour(x$Fecha)))
  updt2 <- updt[updt$Nombre %in% names(sets),]
  sets <- map2(sets, updt2$maxFecha, ~filter(.x, Fecha > .y))
  sets <- lapply(sets, function(x) {if(nrow(x) == 0) x <- NULL; x})
  sets <- compact(sets)
  Estacion2 <- Estacion[Estacion$Nombre %in% names(sets),]
  sets <- map2(sets, Estacion2$Nombre, ~mutate(.x, Monitor = .y))
  sets <- map2(sets, Estacion2$Comuna, ~mutate(.x, Comuna = .y))
  sets <- map2(sets, Estacion2$Codigo, ~mutate(.x, Codigo = .y))
  sets <- map2(sets, Estacion2$Region, ~mutate(.x, Region = .y))
}

#Obtener set de datos meteorológicos
getMetData <- function(){
  sets <- lapply(url, read.csv2, header = TRUE, 
                 colClasses = c(rep("integer",2), "numeric", "NULL"), col.names = c("FechaOrig", "Hora", "vbl", "x")
                 , row.names = NULL)
  names(sets) <- Estacion$Nombre
  sets <- lapply(sets, function(x) x[!is.na(x$vbl),])
  sets <- lapply(sets, function(x) {if(nrow(x) == 0) x <- NULL; x})
  sets <- compact(sets)
  sets <- lapply(sets, function(x) mutate(x, Fechatmp = paste0(sprintf("%06.0f", x$FechaOrig), sprintf("%04.0f", x$Hora))))
  sets <- lapply(sets, function(x) mutate(x, Fecha = ymd_hm(x$Fechatmp, tz = "America/Santiago")))
  sets <- lapply(sets, function(x) mutate(x, Ano = year(x$Fecha), Mes = month(x$Fecha), Dia = day(x$Fecha), Hora = hour(x$Fecha)))
  updt2 <- updt[updt$Nombre %in% names(sets),]
  sets <- map2(sets, updt2$maxFecha, ~filter(.x, Fecha > .y))
  sets <- lapply(sets, function(x) {if(nrow(x) == 0) x <- NULL; x})
  sets <- compact(sets)
  Estacion2 <- Estacion[Estacion$Nombre %in% names(sets),]
  sets <- map2(sets, Estacion2$Nombre, ~mutate(.x, Monitor = .y))
  sets <- map2(sets, Estacion2$Comuna, ~mutate(.x, Comuna = .y))
  sets <- map2(sets, Estacion2$Codigo, ~mutate(.x, Codigo = .y))
  sets <- map2(sets, Estacion2$Region, ~mutate(.x, Region = .y))
}

#Ajuste de formatos datos calidad
cleanCalData <- function(data, vbl){
  data$TipoDato <- sub("Registros.validados", "Validado", data[,3])
  data$TipoDato <- sub("Registros.no.validados", "No validado", data[,3])
  data$TipoDato <- sub("Registros.preliminares", "Preliminar", data[,3])
  data$Ano <- as.integer(data$Ano)
  data$Mes <- as.integer(data$Mes)
  colnames(data)[4] <- paste0("Con", vbl)
  return(data)
}

#Ajuste de formatos datos meteorológicos
cleanMetData <- function(data, var){
  data$Ano <- as.integer(data$Ano)
  data$Mes <- as.integer(data$Mes)
  colnames(data)[3] <- var
  return(data)
}