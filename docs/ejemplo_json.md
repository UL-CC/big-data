# Ejemplo JSON

```json
{
  "hospital": {
    "id": "HSP-001",
    "nombre": "Hospital Universitario del Valle",
    "ubicacion": "Cali, Colombia"
  },
  "sesion_monitoreo": {
    "id": "SES-20250530-001",
    "fecha_inicio": "2025-05-30T08:00:00Z",
    "fecha_fin": "2025-05-30T20:00:00Z",
    "unidad": "UCI",
    "habitacion": "UCI-205"
  },
  "paciente": {
    "id": "P12345",
    "historia_clinica": "HC-789456",
    "edad": 45,
    "sexo": "M",
    "peso_kg": 75.5,
    "altura_cm": 175,
    "condicion_principal": "Post-operatorio cardiovascular"
  },
  "dispositivos_monitoreados": [
    {
      "dispositivo_id": "MON-CV-001",
      "fabricante": "Philips",
      "modelo": "IntelliVue MX800",
      "ubicacion": "Cabecera cama",
      "estado": "activo",
      "ultima_calibracion": "2025-05-29T14:30:00Z"
    },
    {
      "dispositivo_id": "VENT-001",
      "fabricante": "Medtronic",
      "modelo": "Puritan Bennett 980",
      "ubicacion": "Soporte ventilatorio",
      "estado": "activo",
      "ultima_mantenimiento": "2025-05-25T09:00:00Z"
    }
  ],
  "registros_sensores": [
    {
      "id": "REG-001",
      "paciente_id": "P12345",
      "dispositivo_id": "MON-CV-001",
      "timestamp": "2025-05-30T10:00:00Z",
      "tipo_sensor": "electrocardiograma",
      "tipo_dato": "frecuencia_cardiaca",
      "valor": 72,
      "unidad": "bpm",
      "rango_normal": {
        "min": 60,
        "max": 100
      },
      "estado_alerta": "normal",
      "calidad_senal": "excelente",
      "metadatos": {
        "derivacion": "Lead II",
        "filtro_aplicado": "50Hz",
        "resolucion": "0.1bpm"
      }
    },
    {
      "id": "REG-002",
      "paciente_id": "P12345",
      "dispositivo_id": "MON-CV-001",
      "timestamp": "2025-05-30T10:00:15Z",
      "tipo_sensor": "presion_arterial",
      "tipo_dato": "presion_arterial_sistolica",
      "valor": 125,
      "unidad": "mmHg",
      "rango_normal": {
        "min": 90,
        "max": 140
      },
      "estado_alerta": "normal",
      "metadatos": {
        "metodo_medicion": "oscilometrico",
        "brazalete_tamano": "adulto_standard",
        "ciclo_medicion": "automatico_5min"
      },
      "medicion_completa": {
        "sistolica": 125,
        "diastolica": 78,
        "presion_media": 94,
        "presion_pulso": 47
      }
    },
    {
      "id": "REG-003",
      "paciente_id": "P12345",
      "dispositivo_id": "MON-CV-001",
      "timestamp": "2025-05-30T10:01:00Z",
      "tipo_sensor": "oximetria_pulso",
      "tipo_dato": "saturacion_oxigeno",
      "valor": 98,
      "unidad": "%",
      "rango_normal": {
        "min": 95,
        "max": 100
      },
      "estado_alerta": "normal",
      "calidad_senal": "buena",
      "metadatos": {
        "longitud_onda": "660nm/940nm",
        "ubicacion_sensor": "dedo_indice_derecho",
        "perfusion_index": 2.1,
        "frecuencia_pulso": 73
      }
    },
    {
      "id": "REG-004",
      "paciente_id": "P12345",
      "dispositivo_id": "MON-CV-001",
      "timestamp": "2025-05-30T10:02:00Z",
      "tipo_sensor": "temperatura",
      "tipo_dato": "temperatura_corporal",
      "valor": 36.8,
      "unidad": "°C",
      "rango_normal": {
        "min": 36.0,
        "max": 37.5
      },
      "estado_alerta": "normal",
      "metadatos": {
        "ubicacion_medicion": "timpano",
        "sensor_tipo": "infrarrojo",
        "compensacion_ambiental": true
      }
    },
    {
      "id": "REG-005",
      "paciente_id": "P12345",
      "dispositivo_id": "VENT-001",
      "timestamp": "2025-05-30T10:03:00Z",
      "tipo_sensor": "ventilador_mecanico",
      "tipo_dato": "parametros_ventilatorios",
      "metadatos": {
        "modo_ventilacion": "SIMV",
        "trigger_sensibilidad": "-2cmH2O"
      },
      "parametros": {
        "volumen_corriente": {
          "valor": 450,
          "unidad": "ml",
          "programado": 450,
          "medido": 448
        },
        "frecuencia_respiratoria": {
          "valor": 16,
          "unidad": "rpm",
          "programado": 16,
          "total": 18,
          "espontanea": 2
        },
        "presion_pico": {
          "valor": 28,
          "unidad": "cmH2O",
          "limite_alarma": 35
        },
        "presion_meseta": {
          "valor": 22,
          "unidad": "cmH2O"
        },
        "peep": {
          "valor": 8,
          "unidad": "cmH2O",
          "programado": 8
        },
        "fio2": {
          "valor": 45,
          "unidad": "%",
          "programado": 45
        },
        "compliance": {
          "valor": 35,
          "unidad": "ml/cmH2O"
        }
      },
      "estado_alerta": "normal"
    },
    {
      "id": "REG-006",
      "paciente_id": "P12345",
      "dispositivo_id": "MON-CV-001",
      "timestamp": "2025-05-30T10:15:30Z",
      "tipo_sensor": "electrocardiograma",
      "tipo_dato": "arritmia_detectada",
      "valor": "PVC_aislado",
      "severidad": "leve",
      "estado_alerta": "atencion",
      "metadatos": {
        "derivacion": "Lead V1",
        "duracion_evento": "0.12s",
        "acoplamiento": "tardio",
        "morfologia": "unifocal"
      },
      "contexto_evento": {
        "fc_antes": 74,
        "fc_despues": 71,
        "timestamp_anterior": "2025-05-30T10:15:25Z"
      }
    },
    {
      "id": "REG-007",
      "paciente_id": "P12345",
      "dispositivo_id": "MON-CV-001",
      "timestamp": "2025-05-30T10:45:00Z",
      "tipo_sensor": "presion_arterial",
      "tipo_dato": "presion_arterial_sistolica",
      "valor": 155,
      "unidad": "mmHg",
      "rango_normal": {
        "min": 90,
        "max": 140
      },
      "estado_alerta": "alerta_alta",
      "nivel_prioridad": "media",
      "medicion_completa": {
        "sistolica": 155,
        "diastolica": 92,
        "presion_media": 113,
        "presion_pulso": 63
      },
      "acciones_automaticas": [
        "notificacion_enfermeria",
        "registro_tendencia",
        "sugerencia_remedicion_5min"
      ],
      "confirmacion_requerida": true
    }
  ],
  "alertas_activas": [
    {
      "id": "ALT-001",
      "timestamp": "2025-05-30T10:45:00Z",
      "tipo": "presion_arterial_elevada",
      "prioridad": "media",
      "parametro_afectado": "presion_arterial_sistolica",
      "valor_actual": 155,
      "valor_umbral": 140,
      "estado": "activa",
      "acciones_tomadas": [
        "notificacion_enfermera_turno",
        "registro_evento_historial"
      ],
      "tiempo_desde_activacion": "00:00:30"
    }
  ],
  "estadisticas_sesion": {
    "total_registros": 7,
    "registros_por_tipo": {
      "frecuencia_cardiaca": 1,
      "presion_arterial": 2,
      "saturacion_oxigeno": 1,
      "temperatura": 1,
      "ventilacion": 1,
      "eventos_especiales": 1
    },
    "alertas_generadas": 1,
    "calidad_datos": {
      "excelente": 4,
      "buena": 2,
      "regular": 1
    },
    "tiempo_monitoreo_minutos": 45
  },
  "configuracion_sistema": {
    "frecuencia_muestreo": {
      "signos_vitales": "15s",
      "ventilacion": "60s",
      "ecg_continuo": "1s"
    },
    "umbrales_personalizados": {
      "fc_min": 55,
      "fc_max": 110,
      "pa_sistolica_max": 150,
      "spo2_min": 92
    },
    "notificaciones": {
      "audio": true,
      "visual": true,
      "remota": true
    }
  }
}
```
