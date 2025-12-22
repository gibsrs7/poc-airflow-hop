library(httr2)
suppressPackageStartupMessages(library(tidyverse))

parse_data <- function(req){
  
  dados_resposta <- req_perform(req) |> 
    httr2::resp_body_json(simplifyVector = TRUE) |> 
    pluck("dados")
  
  return(dados_resposta)
  
}

request_create <- function (url_base,path,query=NULL,token=NULL){
  
  req <- request(url_base)|> 
    req_url_path_append(path) 
  
  if(!is.null(token) && token != ""){
    req <- req |> 
      req_auth_bearer_token(token)
  }
  
  if(!is.null(query) && query != ""){
    req <- req |> 
      req_url_query(!!!query, .multi= "comma")
  }
  
  
  return(req)
}

url_base <- "https://dadosabertos.camara.leg.br/api/v2/"
path_deputados <- c("deputados")
query_deputados <- list(id="204379")

tabela_deputados <- 
  url_base |> 
  request_create(path_deputados) |> 
  parse_data()


# Coletar uma lista de alguns deputados

id_deputados <- tabela_deputados$id[5:25]
  
query_gastos <- list(mes=c(1,3,6,9,10,11))

lista_tabelas_gastos <- list()

  
for(id_atual in id_deputados){
  
  path_gastos <- c("deputados",id_atual,"despesas")
  
  tryCatch({
    
    tabela_atual <- 
      url_base |> 
      request_create(path_gastos) |> 
      parse_data() |> 
      as_tibble()}, error = function(e) {
        print(paste("❌ Erro ao baixar ID:", id_atual))
        return(NULL) # Retornamos NULL para indicar falha
      }
  )
    
    if(nrow(tabela_atual) > 0){
      tabela_atual <- tabela_atual |> 
        mutate(id_deputado = id_atual)
      
      lista_tabelas_gastos[[as.character(id_atual)]] <- tabela_atual
    }
  
  Sys.sleep(0.5)
  
}

tabela_gastos <- bind_rows(lista_tabelas_gastos)

arquivo_deputados <- "/dados/tabela_deputados.csv"
  
arquivo_gastos <- "/dados/tabela_gastos.csv"

write_csv2(tabela_deputados,arquivo_deputados)

write_csv2(tabela_gastos,arquivo_gastos)



