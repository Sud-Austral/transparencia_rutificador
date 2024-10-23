CREATE TABLE IF NOT EXISTS PERSONAL (
    id SERIAL PRIMARY KEY,
    organismo_nombre VARCHAR(200),
    anyo NUMERIC(6,2),
    Mes VARCHAR(200),
    tipo_calificacionp VARCHAR(500),
    tipo_cargo VARCHAR(200),
    remuneracionbruta_mensual NUMERIC(12),
    remuliquida_mensual NUMERIC(12),
    base VARCHAR(200),
    tipo_pago VARCHAR(200),
    num_cuotas NUMERIC(6,2),
    nombrecompleto_x VARCHAR(200),
    rut VARCHAR(200),
    nombreencontrado VARCHAR(200),
    cantidad_de_pago NUMERIC(6,2),
    detalle_de_base NUMERIC(6,2),
    tipo_de_contrato NUMERIC(6,2),
    homologado VARCHAR(200),
    homologado_2 VARCHAR(200),
    key VARCHAR(200),
    metodo VARCHAR(200)
);


CREATE TABLE IF NOT EXISTS PERSONAL2 (
    id SERIAL PRIMARY KEY,
    organismo_nombre VARCHAR(200),  -- Tamaño ajustado
    anyo SMALLINT,  -- Cambio de NUMERIC(6,2) a SMALLINT
    mes VARCHAR(100),  -- Cambio de VARCHAR a SMALLINT (representación de mes)
    tipo_calificacionp TEXT,  -- Uso de TEXT para mayor flexibilidad
    tipo_cargo VARCHAR(200),
    remuneracionbruta_mensual NUMERIC(12,2),
    remuliquida_mensual NUMERIC(12,2),
    base TEXT,
    tipo_pago TEXT,
    num_cuotas NUMERIC(6,2),
    nombrecompleto_x VARCHAR(200),
    rut VARCHAR(20) NOT NULL,  -- Añadir NOT NULL si el campo es requerido
    nombreencontrado VARCHAR(200),
    cantidad_de_pago NUMERIC(6,2),
    detalle_de_base NUMERIC(6,2),
    tipo_de_contrato NUMERIC(6,2),
    homologado VARCHAR(100),
    homologado_2 VARCHAR(100),
    key VARCHAR(100),
    metodo VARCHAR(100)
);


CREATE TABLE acumulado_rutificador (
    id SERIAL PRIMARY KEY,          -- Identificador único autoincremental
    Fecha DATE,                     -- Fecha de registro
    Homologado VARCHAR(150),         -- Tipo de "Homologado", por ejemplo, 'Alumno(a)'
    base VARCHAR(50),               -- El campo base (como 'Codigotrabajo')
    rut_in INT,                     -- Número de entradas (entero)
    rut_out INT,                    -- Número de salidas (entero)
    Diferencia INT,                 -- Diferencia entre entradas y salidas
    Acumulado INT                   -- Acumulado parcial
);

ALTER TABLE acumulado_rutificador
ADD COLUMN nueva_columna VARCHAR(150);




CREATE TABLE acumulado_rut_detallle_entrada_salida (
    id SERIAL PRIMARY KEY,          -- Identificador único autoincremental
    rut VARCHAR(50),
    fecha_in DATE,
    remuneracionbruta_mensual_in BIGINT,
    remuliquida_mensual_in BIGINT,
    base_in VARCHAR(150),
    homologado_in VARCHAR(150),
    homologado2_in VARCHAR(150),
    fecha_out DATE,
    remuneracionbruta_mensual_out BIGINT,
    remuliquida_mensual_out BIGINT,
    base_out VARCHAR(150),
    homologado_out VARCHAR(150),
    homologado2_out VARCHAR(150),
    nombre_completo VARCHAR(250),
    organismo VARCHAR(250)
);

ALTER TABLE acumulado_rut_detallle_entrada_salida
ADD COLUMN nombre_completo VARCHAR(250);

CREATE TABLE acumulado_resumen_rut_homolago_acumulado (
    id SERIAL PRIMARY KEY,          -- Identificador único autoincremental
    fecha DATE,
    base VARCHAR(150),
    homologado VARCHAR(150),
    rut_in  INT,
    remuneracionbruta_mensual_in BIGINT,
    remuliquida_mensual_in BIGINT,
    rut_out  INT,
    remuneracionbruta_mensual_out BIGINT,
    remuliquida_mensual_out BIGINT,
    homologado_in INT,
    homologado_out INT,
    homologado_diferencia INT,
    homologado_acumulado INT,
    organismo VARCHAR(250)
);

CREATE TABLE acumulado_resumen_solo_rut_acumulado (
    id SERIAL PRIMARY KEY,          -- Identificador único autoincremental
    fecha DATE,
    base VARCHAR(150),
    rut_in  INT,
    remuneracionbruta_mensual_in BIGINT,
    remuliquida_mensual_in BIGINT,
    rut_out  INT,
    remuneracionbruta_mensual_out BIGINT,
    remuliquida_mensual_out BIGINT,
    diferencia INT,
    acumulado INT,
    nombre_completo VARCHAR(250),
    organismo VARCHAR(250)
);

CREATE TABLE resumen_pago (
    id SERIAL PRIMARY KEY,          -- Identificador único autoincremental
    organismo_nombre VARCHAR(250),
    anyo  INT,
    rut_unique_count INT,
    remuneracionbruta_sum BIGINT,
    remuliquida_sum BIGINT,
    meses_unicos  INT,
    pagos BIGINT,
    max_teorico BIGINT,
    dif_pagos INT,
    pago_x_persona NUMERIC(18, 6);
);

CREATE TABLE organismo360 (
    id SERIAL PRIMARY KEY,          -- Identificador único autoincremental
    codigo_org VARCHAR(250),
    idorg VARCHAR(250),
    organismo VARCHAR(250),
    codigo_padre VARCHAR(250),
    padre_org VARCHAR(250),
    region VARCHAR(250),
    municipalidad VARCHAR(250),
    direccion VARCHAR(250),
    telefono VARCHAR(250),

    url_organismo VARCHAR(250),
    url_orgtransparencia VARCHAR(250),
    horario_publico VARCHAR(350),
    nombre_encargado VARCHAR(250),
    email VARCHAR(250),

    num_cuenta VARCHAR(250),
    rut VARCHAR(250),
    tipo_cuenta VARCHAR(250),
    banco VARCHAR(250),
    url_sai VARCHAR(250),

    fax VARCHAR(250),
    ingresa VARCHAR(250),
    obligadorecibir_sai VARCHAR(250),
    organismo_autonomo VARCHAR(250),

    interopera VARCHAR(250),
    tiene_ta VARCHAR(250),
    fecha_ta DATE,
    activado VARCHAR(250)
);



