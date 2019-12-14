#Crear URL para descarga de contaminantes
makeUrlCal <- function(region, param, from = NULL, to = NULL){
  url1 <-paste0("http://sinca.mma.gob.cl/cgi-bin/APUB-MMA/apub.tsindico2.cgi?outtype=xcl&macro=./",region,"/")
  if(is.null(from) | is.null(to)){
    url2 <- paste0("/Cal/",param,"//",param,".horario.horario.ic")
  } else {
    url2 <- paste0("/Cal/",param,"//",param,".horario.horario.ic&from=", from, "&to=", to)
  }
  url <- lapply(Estacion$Carpeta, function(x) paste0(url1, x, url2))
  return(url)
}

#Crear URL para descarga de variables meteorológicas
makeUrlMet <- function(region, param, from = NULL, to = NULL){
  url1 <-paste0("http://sinca.mma.gob.cl/cgi-bin/APUB-MMA/apub.tsindico2.cgi?outtype=xcl&macro=./",region,"/")
  if(is.null(from) | is.null(to)){
    url2 <- ifelse(param == "WDIR", paste0("/Met/",param,"//horario_000_spec.ic"), paste0("/Met/",param,"//horario_000.ic"))
  } else {
    url2 <- ifelse(param == "WDIR", paste0("/Met/",param,"//horario_000_spec.ic&from=", from, "&to=", to), paste0("/Met/",param,"//horario_000.ic&from=", from, "&to=", to))
  }
  url <- lapply(Estacion$Carpeta, function(x) paste0(url1, x, url2))
  return(url)
}

#Obtener set de datos de calidad
getCalData <- function(){
  sets <- lapply(url, read.csv2, header = TRUE, 
               colClasses = c("integer", "integer",rep("numeric",3), "NULL"), dec = ",")
  names(sets) <- Estacion$Nombre
  sets <- lapply(sets, function(x) {if(nrow(x) == 0) x <- NULL; x})
  sets <- compact(sets)
  sets <- lapply(sets, function(x) gather(x, TipoDato, vbl, 3:5, na.rm = TRUE, factor_key = FALSE))
  sets <- lapply(sets, function(x) {if(nrow(x) == 0) x <- NULL; x})
  sets <- purrr::compact(sets)
  sets <- lapply(sets, function(x) mutate(x, Fechatmp = paste0(sprintf("%06.0f", x$FECHA..YYMMDD.), sprintf("%04.0f", x$HORA..HHMM.))))
  sets <- lapply(sets, function(x) mutate(x, Fecha = ymd_hm(x$Fechatmp, tz = "America/Santiago")))
  sets <- lapply(sets, function(x) mutate(x, Ano = year(x$Fecha), Mes = month(x$Fecha), Dia = day(x$Fecha), Hora = hour(x$Fecha)))
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
  sets <- purrr::compact(sets)
  sets <- lapply(sets, function(x) mutate(x, Fechatmp = paste0(sprintf("%06.0f", x$FechaOrig), sprintf("%04.0f", x$Hora))))
  sets <- lapply(sets, function(x) mutate(x, Fecha = ymd_hm(x$Fechatmp, tz = "America/Santiago")))
  sets <- lapply(sets, function(x) mutate(x, Ano = year(x$Fecha), Mes = month(x$Fecha), Dia = day(x$Fecha), Hora = hour(x$Fecha)))
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