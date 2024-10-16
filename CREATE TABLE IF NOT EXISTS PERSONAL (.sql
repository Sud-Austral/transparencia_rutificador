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