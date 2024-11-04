CREATE TABLE IF NOT EXISTS STOP_CONSOLIDADO (
    id SERIAL PRIMARY KEY,
    semana NUMERIC(18,2),
    semana_ant NUMERIC(18,2),
    semana_var NUMERIC(18,2),
    semana_var_porcentual NUMERIC(18,2),

    mes                             NUMERIC(18,2),
    mes_ant                         NUMERIC(18,2),
    mes_var                         NUMERIC(18,2),
    mes_var_porcentual              NUMERIC(18,2),
    anio                            NUMERIC(18,2),
    anio_ant                        NUMERIC(18,2),
    anio_var                        NUMERIC(18,2),
    anio_var_porcentual             NUMERIC(18,2),

    delito                           VARCHAR(400), 
    informacion                      VARCHAR(400), 
    id_semana                        VARCHAR(400), 
    actual_anio                     VARCHAR(400), 
    anterior_anio                   VARCHAR(400), 
    cuad_titulo                      VARCHAR(400), 
    cua_codigo                        VARCHAR(400), 
    comuna                           VARCHAR(400), 
    unidad                           VARCHAR(400), 
    destacamen                       VARCHAR(400), 
    prefectura                       VARCHAR(400), 
    zona                             VARCHAR(400), 
    num_cuad                         VARCHAR(400), 
    cua_descri                       VARCHAR(400), 
    cua_tipo                         VARCHAR(400), 
    cua_estado                        VARCHAR(400), 
    cua_ftermi                      VARCHAR(400), 
    uni_codigo                        VARCHAR(400), 
    cua_ano                         VARCHAR(400), 
    cod_aupol                         VARCHAR(400), 
    com_codigo                        VARCHAR(400), 
    cod_destac                          VARCHAR(400), 


    fecha_inicial            DATE,
    fecha_final              DATE,

    cod_cuadrante_interno    VARCHAR(400), 
    nombre_archivo           VARCHAR(400)

   
);

