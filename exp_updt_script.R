library(tidyverse)
library(lubridate)
library(stringr)

Estacion <- read_csv("estacion.csv")
ultFecha <- data_frame(Nombre = Estacion$Nombre, ultFechaFmt = "191101")
updt <- dplyr::left_join(Estacion, ultFecha, by = c("Nombre" = "Nombre")) %>% select(Nombre, ultFechaFmt)
ayer <- format(today(tzone = "America/Santiago")-1, "%y%m%d")

url1 <-paste0("http://sinca.mma.gob.cl/cgi-bin/APUB-MMA/apub.tsindico2.cgi?outtype=xcl&macro=./","RM","/")
url2 <- paste0("/Cal/","PM25","//","PM25",".horario.horario.ic")
url <- map2(Estacion$Carpeta, updt$ultFechaFmt, ~paste0(url1, .x, url2, "&from=", .y, "&to=", ayer))
url <- lapply(url, str_replace_all, "NA", "")


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
  #sets <- map2(sets, updt2$maxFecha, ~filter(.x, Fecha > .y))
  sets <- lapply(sets, function(x) {if(nrow(x) == 0) x <- NULL; x})
  sets <- compact(sets)
  Estacion2 <- Estacion[Estacion$Nombre %in% names(sets),]
  sets <- map2(sets, Estacion2$Nombre, ~mutate(.x, Monitor = .y))
  sets <- map2(sets, Estacion2$Comuna, ~mutate(.x, Comuna = .y))
  sets <- map2(sets, Estacion2$Codigo, ~mutate(.x, Codigo = .y))
  sets <- map2(sets, Estacion2$Region, ~mutate(.x, Region = .y))
}

cleanCalData <- function(data, vbl){
  data$TipoDato <- sub("Registros.validados", "Validado", data[,3])
  data$TipoDato <- sub("Registros.no.validados", "No validado", data[,3])
  data$TipoDato <- sub("Registros.preliminares", "Preliminar", data[,3])
  data$Ano <- as.integer(data$Ano)
  data$Mes <- as.integer(data$Mes)
  colnames(data)[4] <- paste0("Con", vbl)
  return(data)
}


sets <- getCalData()
if(length(sets) > 0){
  MP25 <- sets %>% Reduce(function(a,b) dplyr::union(a,b),. ); rm(sets) #Generación de dataframe
  MP25 <- cleanCalData(data = MP25, vbl = "MP25") #Limpieza y ajustes de formato
  MP25 <- select(MP25, Fecha, ConMP25, TipoDato, Ano, Mes, Dia, Hora, Region, Comuna, Monitor, Codigo) #Selección de tabla final
  
  write_csv(MP25, "MP25.csv", append = TRUE)
  }

rm(list = setdiff(ls(), c("Estacion", "MP25")))

   