# Ejemplo XML

```xml
<?xml version="1.0" encoding="UTF-8"?>
<FacturaElectronica xmlns="http://www.facturacion.gov.co/schema" version="2.1">
  
  <!-- Información de la factura -->
  <InformacionGeneral>
    <NumeroFactura>FE-2024-001234</NumeroFactura>
    <FechaEmision>2024-05-30</FechaEmision>
    <HoraEmision>14:30:00</HoraEmision>
    <TipoDocumento>01</TipoDocumento> <!-- 01 = Factura de Venta -->
    <Moneda>COP</Moneda>
    <CUFE>a1b2c3d4e5f6789012345678901234567890abcd</CUFE>
  </InformacionGeneral>

  <!-- Datos del emisor -->
  <Emisor>
    <RazonSocial>Tecnología y Soluciones SAS</RazonSocial>
    <NIT>900123456-1</NIT>
    <RegimenFiscal>49</RegimenFiscal> <!-- Responsable del IVA -->
    <Direccion>
      <Calle>Carrera 15 # 93-45</Calle>
      <Ciudad>Bogotá</Ciudad>
      <Departamento>Cundinamarca</Departamento>
      <CodigoPostal>110221</CodigoPostal>
    </Direccion>
    <Telefono>+57-1-2345678</Telefono>
    <Email>facturacion@tecysol.com.co</Email>
  </Emisor>

  <!-- Datos del receptor -->
  <Receptor>
    <RazonSocial>Comercializadora del Valle LTDA</RazonSocial>
    <NIT>800987654-2</NIT>
    <RegimenFiscal>48</RegimenFiscal>
    <Direccion>
      <Calle>Avenida 6N # 25-80</Calle>
      <Ciudad>Cali</Ciudad>
      <Departamento>Valle del Cauca</Departamento>
      <CodigoPostal>760001</CodigoPostal>
    </Direccion>
    <Email>compras@comervalle.com.co</Email>
  </Receptor>

  <!-- Detalle de productos/servicios -->
  <DetalleFactura>
    <Item>
      <NumeroLinea>1</NumeroLinea>
      <CodigoProducto>SOFT-001</CodigoProducto>
      <Descripcion>Licencia de Software ERP - Módulo Contable</Descripcion>
      <Cantidad>2</Cantidad>
      <UnidadMedida>UND</UnidadMedida>
      <ValorUnitario>2500000.00</ValorUnitario>
      <ValorTotal>5000000.00</ValorTotal>
      <Impuestos>
        <IVA>
          <Porcentaje>19.00</Porcentaje>
          <Valor>950000.00</Valor>
        </IVA>
      </Impuestos>
    </Item>
    
    <Item>
      <NumeroLinea>2</NumeroLinea>
      <CodigoProducto>SERV-001</CodigoProducto>
      <Descripcion>Servicio de Implementación y Capacitación</Descripcion>
      <Cantidad>20</Cantidad>
      <UnidadMedida>HOR</UnidadMedida>
      <ValorUnitario>150000.00</ValorUnitario>
      <ValorTotal>3000000.00</ValorTotal>
      <Impuestos>
        <IVA>
          <Porcentaje>19.00</Porcentaje>
          <Valor>570000.00</Valor>
        </IVA>
      </Impuestos>
    </Item>
  </DetalleFactura>

  <!-- Totales de la factura -->
  <Totales>
    <SubTotal>8000000.00</SubTotal>
    <TotalImpuestos>1520000.00</TotalImpuestos>
    <TotalDescuentos>0.00</TotalDescuentos>
    <Total>9520000.00</Total>
    <TotalEnLetras>NUEVE MILLONES QUINIENTOS VEINTE MIL PESOS M/CTE</TotalEnLetras>
  </Totales>

  <!-- Medios de pago -->
  <MediosPago>
    <MedioPago>
      <Codigo>10</Codigo> <!-- Efectivo -->
      <Descripcion>Efectivo</Descripcion>
      <FechaPago>2024-06-15</FechaPago>
    </MedioPago>
  </MediosPago>

  <!-- Información adicional -->
  <InformacionAdicional>
    <CampoAdicional nombre="Orden de Compra">OC-2024-0567</CampoAdicional>
    <CampoAdicional nombre="Vendedor">Juan Pérez</CampoAdicional>
    <CampoAdicional nombre="Observaciones">Entrega en 15 días hábiles</CampoAdicional>
  </InformacionAdicional>

  <!-- Firma digital (representación simplificada) -->
  <FirmaDigital>
    <Certificado>MIICertificadoDigital...</Certificado>
    <Algoritmo>SHA256withRSA</Algoritmo>
    <FechaFirma>2024-05-30T14:30:00Z</FechaFirma>
  </FirmaDigital>

</FacturaElectronica>
```
