# Downloading and decode NOVOCAGED (preimere use in powerbi)

#install.packages(c("tidyverse", "archive", "curl"))

# 4 usar forcats

setwd("X:/POWER BI/NOVOCAGED")

dir.create("data")

# Defining variable parts in the possible universe links

competencias <- c('MOV', 'FOR', 'EXC')
anos <- 2020:as.integer(lubridate::year(lubridate::today()))
meses <- formatC(1:12, width = 2, flag = '0')

# comparing if there are files already

vetor_comparacao <- vector(mode = 'list', length = (length(competencias) *
                                                      length(anos) *
                                                      length(meses)))
counter <- 1  # Initialize a counter variable

for(i in seq_along(competencias)) {
  
  for(j in seq_along(anos)) {
    
    for(k in seq_along(meses)) {
      
      vetor_comparacao[[counter]] <-
        paste0("CAGED", competencias[i], anos[j], meses[k], ".7z")
      
      counter <- counter + 1  # Increment the counter
    }
  }
}

vetor_comparacao <- vetor_comparacao |> unlist()

diretorio_comparacao <- paste0(getwd(),"/data") |> dir()

vetor_download <- vetor_comparacao[!vetor_comparacao %in% diretorio_comparacao]

# Applying command Loop to download plain text files

for(i in seq_along(vetor_download)){
  
  tryCatch({
    curl::curl_download(
      paste0("ftp://ftp.mtps.gov.br/pdet/microdados/NOVO%20CAGED/",
             stringr::str_extract(vetor_download[i], "\\d{4}"), "/", # year
             stringr::str_extract(vetor_download[i], "\\d{6}"), "/", # yearmonth
             vetor_download[i]), # file name
      quiet = T,
      destfile = paste0("data/", vetor_download[i]))
  }, error = function(err) {warning("file could not be downloaded")})
}

# Reading and unifying files of same nature

setwd('data')

# creating a vector of same nature file

for(k in seq_along(competencias)){
  
  # cagedxxx_baixadas <- fs::dir_ls(glob = "CAGEDXXX*.7z$")
  
  assign(tolower(paste0('caged', competencias[k], '_baixadas')),
         fs::dir_ls(glob = paste0('CAGED', competencias[k], '*.7z$')))
  
  # cagedxxx_lista <- list()
  
  assign(tolower(paste0('caged', competencias[k], '_lista')),
         list())
}

# Reading archives

arquivos_caged <-
  function(entrada) {
    
    if(!any(entrada == c('MOV', 'FOR', 'EXC'))) {
      stop("Competencia deve ser FOR, MOV ou EXC", call. = FALSE)
    }
    
    caminho_dos_arquivos <- get(paste0('caged', tolower(entrada), '_baixadas'))
    lista_arquivos_periodo <- vector(mode = 'list',
                                     length = length(caminho_dos_arquivos))
    
    for(l in seq_along(caminho_dos_arquivos)) {
      
      lista_arquivos_periodo[[l]] <-
        readr::read_csv2(archive::archive_read(caminho_dos_arquivos[l]),
                         col_select = !c("região", "seção",
                                         "origemdainformação", "competênciadec",
                                         "indicadordeforadoprazo"),
                         col_types = readr::cols(
                           "competênciamov" = readr::col_date(format = "%Y%m"),
                           "idade" = readr::col_double(),
                           "horascontratuais" = readr::col_double(),
                           "salário" = readr::col_double(),
                           "valorsaláriofixo" = readr::col_double(),
                           .default = readr::col_character()
                         )
        ) |>
        
        dplyr::filter(uf == 51) # make the file ligther
    }
    
    names(lista_arquivos_periodo) <-
      gsub('.7z', '', as.character(caminho_dos_arquivos))
    
    return(lista_arquivos_periodo)
  }

cagedmov <-
  arquivos_caged("MOV") |> dplyr::bind_rows()

cagedfor <-
  arquivos_caged("FOR") |> dplyr::bind_rows()

cagedexc <-
  arquivos_caged("EXC") |> dplyr::bind_rows()

# Merging NOVOCAGED

novocaged <-
  cagedmov |> dplyr::bind_rows(cagedfor) |>
  dplyr::anti_join(cagedexc)

# Decoder download
## run this code part only once ##

url_dicionario <- 
  paste0("ftp://ftp.mtps.gov.br/pdet/microdados/NOVO%20CAGED/",
         "Layout%20N%E3o-identificado%20Novo%20Caged%20Movimenta%E7%E3o.xlsx")

curl::curl_download(url_dicionario, destfile = "layout.xlsx")

# building the decoder

# exploring the existing variables in the decode
folhas_tradutor <- "layout.xlsx" |> readxl::excel_sheets()

# exploring the existing variables in novocaged file
conteudo_asertraduzido <- novocaged |> names()

# defining the variables who should be decoded
nomes_atraduzir <-
  folhas_tradutor[(folhas_tradutor %in% conteudo_asertraduzido)] |> as.list()

# creating a list with every decoder sheet
lista_tradutor <- vector(mode = 'list',
                         length = length(nomes_atraduzir))

nomes_atraduzir <- nomes_atraduzir |> unlist()

# creating a list with every sheets of the decoder
for(l in seq_along(nomes_atraduzir)){
  
  lista_tradutor[[l]] <-
    readxl::read_excel("layout.xlsx", sheet = nomes_atraduzir[[l]],
                       col_types = "text")
}

# renaming the variables in the decoder to be compatible with novocaged file

lista_tradutor_renomeada <- 
  vector(mode = "list", length = length(lista_tradutor))

##### debugging with chat gpt code  using tidyverse

for (l in seq_along(lista_tradutor)){
  lista_tradutor_renomeada[[l]] <- lista_tradutor[[l]] |>
    tibble::as_tibble() |>
    dplyr::rename_with(~ nomes_atraduzir[l], .cols = "Código") |>
    dplyr::rename_with(~ paste0(nomes_atraduzir[l], "_descricao"), .cols = "Descrição")
}

# Decoding data part by part to be able to safe in a list

compilado_traduzido <- vector(mode = 'list',
                              length = length(lista_tradutor_renomeada))

for(l in seq_along(lista_tradutor_renomeada)){
  compilado_traduzido[[l]] <-
    novocaged[nomes_atraduzir[l]] |>
    dplyr::left_join(lista_tradutor_renomeada[[l]])
}

# Unifying and selecting every decoded data into one file (sera que consigo usar pipe na parte seguinte?)

compilado_traduzido <-
  compilado_traduzido |> purrr::list_cbind()

compilado_traduzido <-
  compilado_traduzido |> dplyr::select(ends_with("_descricao"))

# Merging decoded file with not decodable part of novocaged

novocaged_decodificado <-
  novocaged |> dplyr::select(c("competênciamov", "município",
                               "saldomovimentação", "subclasse",
                               "cbo2002ocupação", "idade", "horascontratuais",
                               "salário", "valorsaláriofixo")) |>
  dplyr::bind_cols(compilado_traduzido)

# Adding "admission and dismissal" decoding

novocaged_decodificado <-
  novocaged_decodificado |>
  dplyr::mutate(saldomovimentacao_decodificado = dplyr::case_when(
    saldomovimentação == 1 ~ "Admissão",
    saldomovimentação == -1 ~ "Desligamento"
  ), .keep = "all"
  )

# Base code to download, merge, and decode novocaged ready

novocaged_decodificado |> dplyr::glimpse()

# Grouping values of some variables (to use powerbi)

# Grouping age

novocaged_decodificado <- novocaged_decodificado |>
  dplyr::mutate(idade_faixa =
                  dplyr::case_when(
                    idade <= 16 ~ "Menor ou igual 16 anos",
                    idade <= 25 ~ "Maior de 16 até 25 anos",
                    idade <= 35 ~ "Maior de 25 até 35 anos",
                    idade <= 45 ~ "Maior de 35 até 45 anos",
                    idade <= 55 ~ "Maior de 45 até 55 anos",
                    idade <= 65 ~ "Maior de 55 até 65 anos",
                    idade <= 75 ~ "Maior de 65 até 75 anos",
                    idade > 75 ~ "Maior de 75 anos"
                  ), .keep = "all")

# Grouping contract hours

novocaged_decodificado <- novocaged_decodificado |>
  dplyr::mutate(horascontratuais_faixa =
                  dplyr::case_when(
                    horascontratuais <= 14 ~ "Até 14 horas",
                    horascontratuais <= 39 ~ "Mais de 14 até 39 horas",
                    horascontratuais <= 44 ~ "Mais de 39 até 44 horas",
                    horascontratuais <= 48 ~ "Mais de 44 até 48 horas",
                    horascontratuais > 48 ~ "Mais de 48 horas"
                  ), .keep = "all")

# Grouping salary

novocaged_decodificado <- novocaged_decodificado |>
  dplyr::mutate(salário_faixa =
                  dplyr::case_when(
                    salário <= 303 ~ "Menor ou igual R$ 303,00",
                    salário <= 606 ~ "Maior que R$ 303,00 a R$ 606,00",
                    salário <= 1212 ~ "Maior que R$ 606,00 a R$ 1.212,00",
                    salário <= 3636 ~ "Maior que R$ 1.212,00 a R$ 3.636,00",
                    salário <= 6060 ~ "Maior que R$ 3.636,00 a R$ 6.060,00",
                    salário <= 12120 ~ "Maior que R$ 6.060,00 a R$ 12.120,00",
                    salário <= 24240 ~ "Maior que R$ 12.120,00 a R$ 24.240,00",
                    salário > 24240 ~ "Maior que R$ 24.240,00"
                  ), .keep = "all"
  )

# Grouping fixed salary value

novocaged_decodificado <- novocaged_decodificado |>
  dplyr::mutate(valorsaláriofixo_faixa =
                  dplyr::case_when(
                    valorsaláriofixo <= 303 ~ "Menor ou igual R$ 303,00",
                    valorsaláriofixo <= 606 ~ "Maior que R$ 303,00 a R$ 606,00",
                    valorsaláriofixo <= 1212 ~ "Maior que R$ 606,00 a R$ 1.212,00",
                    valorsaláriofixo <= 3636 ~ "Maior que R$ 1.212,00 a R$ 3.636,00",
                    valorsaláriofixo <= 6060 ~ "Maior que R$ 3.636,00 a R$ 6.060,00",
                    valorsaláriofixo <= 12120 ~ "Maior que R$ 6.060,00 a R$ 12.120,00",
                    valorsaláriofixo <= 24240 ~ "Maior que R$ 12.120,00 a R$ 24.240,00",
                    valorsaláriofixo > 24240 ~ "Maior que R$ 24.240,00"
                  ), .keep = "all"
  )

# Sorting data demo (to use in novocaged)

# Sorting age

novocaged_decodificado <- novocaged_decodificado |>
  dplyr::mutate(idade_faixa_ordem =
                  dplyr::case_when(
                    idade_faixa == "Menor ou igual 16 anos" ~ 1,
                    idade_faixa == "Maior de 16 até 25 anos" ~ 2,
                    idade_faixa == "Maior de 25 até 35 anos" ~ 3,
                    idade_faixa == "Maior de 35 até 45 anos" ~ 4,
                    idade_faixa == "Maior de 45 até 55 anos" ~ 5,
                    idade_faixa == "Maior de 55 até 65 anos" ~ 6,
                    idade_faixa == "Maior de 65 até 75 anos" ~ 7,
                    idade_faixa == "Maior de 75 anos" ~ 8
                  ), .keep = "all")

# Sorting contractual hours

novocaged_decodificado <- novocaged_decodificado |>
  dplyr::mutate(horascontratuais_faixa_ordem =
                  dplyr::case_when(
                    horascontratuais_faixa == "Até 14 horas" ~ 1,
                    horascontratuais_faixa == "Mais de 14 até 39 horas" ~ 2,
                    horascontratuais_faixa == "Mais de 39 até 44 horas" ~ 3,
                    horascontratuais_faixa == "Mais de 44 até 48 horas" ~ 4,
                    horascontratuais_faixa == "Mais de 48 horas" ~ 5
                  ), .keep = "all")


# Sorting salary

novocaged_decodificado <- novocaged_decodificado |>
  dplyr::mutate(salário_faixa_ordem =
                  dplyr::case_when(
                    salário_faixa == "Menor ou igual R$ 303,00" ~ 1,
                    salário_faixa == "Maior que R$ 303,00 a R$ 606,00" ~ 2,
                    salário_faixa == "Maior que R$ 606,00 a R$ 1.212,00" ~ 3,
                    salário_faixa == "Maior que R$ 1.212,00 a R$ 3.636,00" ~ 4,
                    salário_faixa == "Maior que R$ 3.636,00 a R$ 6.060,00" ~ 5,
                    salário_faixa == "Maior que R$ 6.060,00 a R$ 12.120,00" ~ 6,
                    salário_faixa == "Maior que R$ 12.120,00 a R$ 24.240,00" ~ 7,
                    salário_faixa == "Maior que R$ 24.240,00" ~ 8
                  ), .keep = "all"
  )

# Sorting fixed salary value

novocaged_decodificado <- novocaged_decodificado |>
  dplyr::mutate(valorsaláriofixo_faixa_ordem =
                  dplyr::case_when(
                    valorsaláriofixo_faixa == "Menor ou igual R$ 303,00" ~ 1,
                    valorsaláriofixo_faixa == "Maior que R$ 303,00 a R$ 606,00" ~ 2,
                    valorsaláriofixo_faixa == "Maior que R$ 606,00 a R$ 1.212,00" ~ 3,
                    valorsaláriofixo_faixa == "Maior que R$ 1.212,00 a R$ 3.636,00" ~ 4,
                    valorsaláriofixo_faixa == "Maior que R$ 3.636,00 a R$ 6.060,00" ~ 5,
                    valorsaláriofixo_faixa == "Maior que R$ 6.060,00 a R$ 12.120,00" ~ 6,
                    valorsaláriofixo_faixa == "Maior que R$ 12.120,00 a R$ 24.240,00" ~ 7,
                    valorsaláriofixo_faixa == "Maior que R$ 24.240,00" ~ 8
                  ), .keep = "all"
  )


# Sorting level of education

novocaged_decodificado <- novocaged_decodificado |>
  dplyr::mutate(graudeinstrução_descricao_ordem =
                  dplyr::case_when(
                    graudeinstrução_descricao == "Analfabeto" ~ 1,
                    graudeinstrução_descricao == "Até 5ª Incompleto" ~ 2,
                    graudeinstrução_descricao == "5ª Completo Fundamental" ~ 3,
                    graudeinstrução_descricao == "6ª a 9ª Fundamental" ~ 4,
                    graudeinstrução_descricao == "Fundamental Completo" ~ 5,
                    graudeinstrução_descricao == "Médio Incompleto" ~ 6,
                    graudeinstrução_descricao == "Médio Completo" ~ 7,
                    graudeinstrução_descricao == "Superior Incompleto" ~ 8,
                    graudeinstrução_descricao == "Superior Completo" ~ 9,
                    graudeinstrução_descricao == "Pós-Graduação completa" ~ 10,
                    graudeinstrução_descricao == "Mestrado" ~ 11,
                    graudeinstrução_descricao == "Doutorado" ~ 12,
                    graudeinstrução_descricao == "Não Identificado" ~ 13,
                  ), .keep = "all"
  )

# Sorting establishment size

novocaged_decodificado <- novocaged_decodificado |>
  dplyr::mutate(tamestabjan_descricao_ordem =
                  dplyr::case_when(
                    tamestabjan_descricao == "Zero" ~ 1,
                    tamestabjan_descricao == "De 1 a 4" ~ 2,
                    tamestabjan_descricao == "De 5 a 9" ~ 3,
                    tamestabjan_descricao == "De 10 a 19" ~ 4,
                    tamestabjan_descricao == "De 20 a 49" ~ 5,
                    tamestabjan_descricao == "De 50 a 99" ~ 6,
                    tamestabjan_descricao == "De 100 a 249" ~ 7,
                    tamestabjan_descricao == "De 250 a 499" ~ 8,
                    tamestabjan_descricao == "De 500 a 999" ~ 9,
                    tamestabjan_descricao == "1000 ou Mais" ~ 10,
                  ), .keep = "all"
  )

# Sorting unit salary code

novocaged_decodificado <- novocaged_decodificado |>
  dplyr::mutate(unidadesaláriocódigo_descricao_ordem =
                  dplyr::case_when(
                    unidadesaláriocódigo_descricao == "Variavel" ~ 1,
                    unidadesaláriocódigo_descricao == "Tarefa" ~ 2,
                    unidadesaláriocódigo_descricao == "Hora" ~ 3,
                    unidadesaláriocódigo_descricao == "Dia" ~ 4,
                    unidadesaláriocódigo_descricao == "Semana" ~ 5,
                    unidadesaláriocódigo_descricao == "Quinzena" ~ 6,
                    unidadesaláriocódigo_descricao == "Mês" ~ 7,
                    unidadesaláriocódigo_descricao == "Não Identificado" ~ 8,
                  ), .keep = "all"
  )

# Using the particular produce decoder to adding more information in the novocaged

compilado_decodificador_endereço <-
  paste0("https://github.com/WillianDambros/data_source/raw/",
         "refs/heads/main/compilado_decodificador.xlsx")

decodificador_endereco <- paste0(getwd(), "/compilado_decodificador.xlsx")

curl::curl_download(compilado_decodificador_endereço,
                    decodificador_endereco)

"compilado_decodificador.xlsx" |> readxl::excel_sheets()

territorialidade_sedec <- 
  readxl::read_excel("compilado_decodificador.xlsx",
                     sheet =  "territorialidade_municipios_mt",
                     col_types = "text") |>
  dplyr::select("territorio_municipio_codigo_7d",
                "territorio_municipionovocaged_codigo_6d",
                "rpseplan10340_munícipio_polo_decodificado",
                "rpseplan10340_regiao_decodificado",
                "imeia_regiao",
                "imeia_municipios_polo_economico",
                "territorio_latitude", "territorio_longitude")

novocaged_decodificado <- novocaged_decodificado |> 
  dplyr::left_join(territorialidade_sedec,
                   by = dplyr::join_by(município == territorio_municipionovocaged_codigo_6d))

# reading, selecting and merging variables about a teme

cnae_sedec <- 
  readxl::read_excel("compilado_decodificador.xlsx",
                     sheet =  "cnae", col_types = "text") |> #dplyr::glimpse()
  dplyr::select("cnae_subclasse_codigo_7d_sem0",
                "cnae_secao_decodificado",
                "cnae_atividades_caracteristicas_turismo",
                "cnae_grande_grupamento_novocaged")

novocaged_decodificado <- novocaged_decodificado |> 
  dplyr::left_join(cnae_sedec,
                   by = dplyr::join_by(subclasse == cnae_subclasse_codigo_7d_sem0))

# reading, selecting and merging variables about a teme

cbo2002_sedec <- 
  readxl::read_excel("compilado_decodificador.xlsx",
                     sheet = "cbo2002", col_types = "text") |> #dplyr::glimpse()
  dplyr::select("cbo_ocupacao_codigo_6d",
                "cbo_grandegrupo_decodificado")

novocaged_decodificado <- novocaged_decodificado |> 
  dplyr::left_join(cbo2002_sedec,
                   by = dplyr::join_by(cbo2002ocupação == cbo_ocupacao_codigo_6d), 
                   multiple = "any")

# Novocaged with additional particular produce information

novocaged_decodificado |> dplyr::glimpse()
novocaged_decodificado$competênciamov |> unique()
# Replacing NA with 0 in numeric variables  

numeric_columns <- novocaged_decodificado |>
  dplyr::select_if(is.numeric) |>
  names()

novocaged_decodificado <- novocaged_decodificado |>
  dplyr::mutate(across(all_of(numeric_columns), ~tidyr::replace_na(.x, 0)))

# Writing novocaged file (estudar incluir o caminho doa rquivo direto na formula)

nome_arquivo_csv <- "novocaged"

#caminho_arquivo <- paste0(stringr::str_remove(getwd(), "data$"), nome_arquivo_csv, ".txt")

#caminho_arquivo <- paste0("D:/comexstat_temporario/", nome_arquivo_csv, ".txt")

#readr::write_csv2(novocaged_decodificado, caminho_arquivo)


novocaged_decodificado$competênciamov |> unique()
# writing PostgreSQL

source("X:/POWER BI/NOVOCAGED/conexao.R")

RPostgres::dbListTables(conexao)

schema_name <- "novo_caged"

table_name <- "novo_caged"

DBI::dbSendQuery(conexao, paste0("CREATE SCHEMA IF NOT EXISTS ", schema_name))

RPostgres::dbWriteTable(conexao,
                        name = DBI::Id(schema = schema_name,table = table_name),
                        value = novocaged_decodificado,
                        row.names = FALSE, overwrite = TRUE)

RPostgres::dbDisconnect(conexao)
################################################################################

endereco <- "X:/POWER BI/NOVOCAGED/estoque.xlsx"
estoque_novocaged <- readxl::read_excel(endereco)

# writing PostgreSQL

source("X:/POWER BI/NOVOCAGED/conexao.R")

RPostgres::dbListTables(conexao)

schema_name <- "novo_caged"

table_name <- "estoque_novocaged"

DBI::dbSendQuery(conexao, paste0("CREATE SCHEMA IF NOT EXISTS ", schema_name))

RPostgres::dbWriteTable(conexao,
                        name = DBI::Id(schema = schema_name,table = table_name),
                        value = estoque_novocaged,
                        row.names = FALSE, overwrite = TRUE)

RPostgres::dbDisconnect(conexao)


ebdereco <- "X:/POWER BI/NOVOCAGED/estoque.xlsx"
novo_caged_estoque <- readxl::read_excel(endereco)


# writing PostgreSQL

source("X:/POWER BI/NOVOCAGED/conexao.R")

RPostgres::dbListTables(conexao)

schema_name <- "novo_caged"

table_name <- "novo_caged_estoque"

DBI::dbSendQuery(conexao, paste0("CREATE SCHEMA IF NOT EXISTS ", schema_name))

RPostgres::dbWriteTable(conexao,
                        name = DBI::Id(schema = schema_name,table = table_name),
                        value = novo_caged_estoque,
                        row.names = FALSE, overwrite = TRUE)

RPostgres::dbDisconnect(conexao)

novocaged_decodificado$competênciamov |> unique()

tempdir()
list.files(tempdir(), full.names = TRUE)
unlink(list.files(tempdir(), full.names = TRUE), recursive = TRUE)
