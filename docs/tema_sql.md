[← Volver al Inicio](index.md)

# Repaso de SQL

## 1. Consultas Básicas (SELECT)

El comando SELECT es la base de toda consulta SQL. Permite recuperar datos de una o más tablas especificando qué columnas mostrar y de qué tablas.

- **Selección básica**

```sql
SELECT nombre, email, edad 
FROM usuarios 
WHERE edad > 25;
```

- **Selección con alias y ordenamiento**

```sql
SELECT 
    nombre AS nombre_completo,
    salario * 12 AS salario_anual
FROM empleados 
ORDER BY salario_anual DESC;
```

## 2. Filtrado de Datos (WHERE)

La cláusula WHERE permite filtrar registros basándose en condiciones específicas. Es esencial para obtener exactamente los datos que necesitas.

- **Filtros con operadores lógicos**

```sql
SELECT * FROM productos 
WHERE precio BETWEEN 100 AND 500 
AND categoria = 'Electrónicos' 
AND stock > 0;
```

- **Filtros con patrones y listas**

```sql
SELECT cliente_id, nombre FROM clientes 
WHERE nombre LIKE 'Juan%' 
OR ciudad IN ('Madrid', 'Barcelona', 'Valencia');
```

## 3. Joins (Uniones de Tablas)

Los joins permiten combinar datos de múltiples tablas relacionadas. Son fundamentales para trabajar con bases de datos normalizadas.

- **INNER JOIN**

```sql
SELECT 
    u.nombre,
    p.titulo,
    p.fecha_publicacion
FROM usuarios u
INNER JOIN posts p ON u.id = p.usuario_id
WHERE p.fecha_publicacion > '2024-01-01';
```

- **LEFT JOIN**

```sql
SELECT 
    c.nombre AS cliente,
    COUNT(p.id) AS total_pedidos
FROM clientes c
LEFT JOIN pedidos p ON c.id = p.cliente_id
GROUP BY c.id, c.nombre;
```

## 4. Funciones de Agregación

Las funciones de agregación realizan cálculos sobre conjuntos de filas y devuelven un único valor: COUNT, SUM, AVG, MIN, MAX.

- **Funciones básicas de agregación**

```sql
SELECT 
    COUNT(*) AS total_productos,
    AVG(precio) AS precio_promedio,
    MAX(precio) AS precio_maximo,
    MIN(stock) AS stock_minimo
FROM productos
WHERE categoria = 'Libros';
```

- **Agregación con GROUP BY**

```sql
SELECT 
    categoria,
    COUNT(*) AS cantidad_productos,
    SUM(precio * stock) AS valor_inventario
FROM productos
GROUP BY categoria
HAVING COUNT(*) > 5;
```

## 5. Agrupación y Filtrado de Grupos (GROUP BY y HAVING)

GROUP BY agrupa filas con valores similares, mientras que HAVING filtra grupos (no filas individuales como WHERE).

- **Agrupación simple**

```sql
SELECT 
    departamento,
    COUNT(*) AS num_empleados,
    AVG(salario) AS salario_promedio
FROM empleados
GROUP BY departamento
ORDER BY salario_promedio DESC;
```

- **Agrupación con filtrado de grupos**

```sql
SELECT 
    YEAR(fecha_pedido) AS año,
    MONTH(fecha_pedido) AS mes,
    SUM(total) AS ventas_mensuales
FROM pedidos
GROUP BY YEAR(fecha_pedido), MONTH(fecha_pedido)
HAVING SUM(total) > 10000;
```

## 6. Inserción de Datos (INSERT)

INSERT permite agregar nuevos registros a las tablas. Es crucial dominar tanto inserciones simples como múltiples.

- **Inserción simple**

```sql
INSERT INTO usuarios (nombre, email, fecha_registro, activo)
VALUES ('María García', 'maria@email.com', '2024-05-31', TRUE);
```

- **Inserción múltiple**

```sql
INSERT INTO productos (nombre, precio, categoria, stock)
VALUES 
    ('iPhone 15', 999.99, 'Electrónicos', 50),
    ('MacBook Pro', 1999.99, 'Electrónicos', 25),
    ('iPad Air', 599.99, 'Electrónicos', 75);
```

## 7. Actualización de Datos (UPDATE)

UPDATE modifica registros existentes. Siempre debe usarse con WHERE para evitar actualizar toda la tabla accidentalmente.

- **Actualización condicional**

```sql
UPDATE empleados 
SET salario = salario * 1.10,
    fecha_actualizacion = NOW()
WHERE departamento = 'Ventas' 
AND fecha_contratacion < '2023-01-01';
```

- **Actualización con subconsulta**

```sql
UPDATE productos 
SET precio = precio * 0.90
WHERE categoria = 'Ropa' 
AND id IN (
    SELECT producto_id 
    FROM inventario 
    WHERE stock > 100
);
```

## 8. Eliminación de Datos (DELETE)

DELETE elimina registros de una tabla. Como UPDATE, siempre debe incluir WHERE para evitar eliminar todos los registros.

- **Eliminación condicional**

```sql
DELETE FROM pedidos 
WHERE estado = 'cancelado' 
AND fecha_pedido < DATE_SUB(NOW(), INTERVAL 1 YEAR);
```

- **Eliminación con subconsulta**

```sql
DELETE FROM usuarios 
WHERE activo = FALSE 
AND id NOT IN (
    SELECT DISTINCT usuario_id 
    FROM pedidos 
    WHERE fecha_pedido > DATE_SUB(NOW(), INTERVAL 6 MONTH)
);
```

## 9. Subconsultas

Las subconsultas son consultas anidadas dentro de otras consultas. Permiten realizar operaciones complejas y comparaciones dinámicas.

- **Subconsulta en WHERE**

```sql
SELECT nombre, salario 
FROM empleados
WHERE salario > (
    SELECT AVG(salario) 
    FROM empleados 
    WHERE departamento = 'IT'
);
```

- **Subconsulta correlacionada**

```sql
SELECT 
    e1.nombre,
    e1.departamento,
    e1.salario
FROM empleados e1
WHERE e1.salario = (
    SELECT MAX(e2.salario)
    FROM empleados e2
    WHERE e2.departamento = e1.departamento
);
```

## 10. Índices y Optimización

Los índices mejoran el rendimiento de las consultas, especialmente en tablas grandes. Es importante saber cuándo y cómo usarlos.

- **Creación de índices**

```sql
-- Índice simple para búsquedas frecuentes
CREATE INDEX idx_usuarios_email ON usuarios(email);

-- Índice compuesto para consultas multi-columna
CREATE INDEX idx_pedidos_fecha_cliente ON pedidos(fecha_pedido, cliente_id);
```

- **Optimización de consultas**

```sql
-- Consulta optimizada usando índices
SELECT * FROM pedidos 
WHERE cliente_id = 123 
AND fecha_pedido BETWEEN '2024-01-01' AND '2024-12-31'
ORDER BY fecha_pedido DESC
LIMIT 10;

-- Uso de EXPLAIN para analizar el plan de ejecución
EXPLAIN SELECT * FROM productos WHERE categoria = 'Libros' AND precio > 20;
```

[← Volver al Inicio](index.md)

