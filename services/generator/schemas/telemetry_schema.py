"""
Telemetry Data Schemas

Pydantic models for car sensor telemetry data used across all services.
"""
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field, ConfigDict


class LocationData(BaseModel):
    """GPS location data"""
    latitude: float = Field(..., ge=-90, le=90, description="Latitude in degrees")
    longitude: float = Field(..., ge=-180, le=180, description="Longitude in degrees")
    altitude_m: Optional[float] = Field(None, description="Altitude in meters")

    model_config = ConfigDict(json_schema_extra={
        "example": {
            "latitude": 37.7749,
            "longitude": -122.4194,
            "altitude_m": 15.5
        }
    })


class SensorData(BaseModel):
    """Raw sensor readings from vehicle"""
    speed_kmh: float = Field(..., ge=0, le=300, description="Speed in km/h")
    rpm: int = Field(..., ge=0, le=8000, description="Engine RPM")
    engine_temp_celsius: float = Field(..., ge=-40, le=150, description="Engine temperature in Celsius")
    fuel_level_percent: float = Field(..., ge=0, le=100, description="Fuel level percentage")
    throttle_position_percent: float = Field(..., ge=0, le=100, description="Throttle position percentage")
    brake_pressure_bar: float = Field(..., ge=0, le=10, description="Brake pressure in bar")
    coolant_temp_celsius: float = Field(..., ge=-40, le=150, description="Coolant temperature in Celsius")
    oil_pressure_bar: float = Field(..., ge=0, le=10, description="Oil pressure in bar")
    battery_voltage: float = Field(..., ge=0, le=20, description="Battery voltage")
    odometer_km: float = Field(..., ge=0, description="Total distance traveled in km")

    model_config = ConfigDict(json_schema_extra={
        "example": {
            "speed_kmh": 65.5,
            "rpm": 2500,
            "engine_temp_celsius": 90.0,
            "fuel_level_percent": 75.0,
            "throttle_position_percent": 45.0,
            "brake_pressure_bar": 0.5,
            "coolant_temp_celsius": 85.0,
            "oil_pressure_bar": 4.5,
            "battery_voltage": 13.8,
            "odometer_km": 45000.0
        }
    })


class RawTelemetry(BaseModel):
    """Raw telemetry data from generator"""
    vehicle_id: str = Field(..., description="Unique vehicle identifier")
    timestamp: datetime = Field(..., description="Event timestamp")
    trace_id: str = Field(..., description="OpenTelemetry trace ID")
    span_id: str = Field(..., description="OpenTelemetry span ID")
    sensor_data: SensorData
    location: LocationData
    metadata: dict = Field(default_factory=dict, description="Additional metadata")

    model_config = ConfigDict(json_schema_extra={
        "example": {
            "vehicle_id": "VEH12345",
            "timestamp": "2026-02-15T10:30:00Z",
            "trace_id": "abc123",
            "span_id": "def456",
            "sensor_data": {
                "speed_kmh": 65.5,
                "rpm": 2500,
                "engine_temp_celsius": 90.0,
                "fuel_level_percent": 75.0,
                "throttle_position_percent": 45.0,
                "brake_pressure_bar": 0.5,
                "coolant_temp_celsius": 85.0,
                "oil_pressure_bar": 4.5,
                "battery_voltage": 13.8,
                "odometer_km": 45000.0
            },
            "location": {
                "latitude": 37.7749,
                "longitude": -122.4194,
                "altitude_m": 15.5
            },
            "metadata": {
                "sensor_version": "v1.0",
                "data_quality_score": 0.95
            }
        }
    })


class DerivedMetrics(BaseModel):
    """Derived metrics calculated from raw sensor data"""
    acceleration_ms2: float = Field(..., description="Acceleration in m/s²")
    fuel_efficiency_kmpl: Optional[float] = Field(None, description="Fuel efficiency in km/L")
    engine_load_percent: float = Field(..., ge=0, le=100, description="Engine load percentage")
    brake_intensity: float = Field(..., ge=0, le=1, description="Brake intensity (0-1)")
    is_idling: bool = Field(..., description="Whether vehicle is idling")
    is_aggressive_driving: bool = Field(..., description="Whether driving pattern is aggressive")

    model_config = ConfigDict(json_schema_extra={
        "example": {
            "acceleration_ms2": 1.5,
            "fuel_efficiency_kmpl": 15.2,
            "engine_load_percent": 55.0,
            "brake_intensity": 0.1,
            "is_idling": False,
            "is_aggressive_driving": False
        }
    })


class AnomalyFlags(BaseModel):
    """Anomaly detection flags"""
    overheating: bool = Field(default=False, description="Engine overheating detected")
    low_oil_pressure: bool = Field(default=False, description="Low oil pressure detected")
    battery_issue: bool = Field(default=False, description="Battery voltage issue detected")
    sudden_deceleration: bool = Field(default=False, description="Sudden deceleration detected")
    fuel_anomaly: bool = Field(default=False, description="Unusual fuel consumption detected")

    model_config = ConfigDict(json_schema_extra={
        "example": {
            "overheating": False,
            "low_oil_pressure": False,
            "battery_issue": False,
            "sudden_deceleration": False,
            "fuel_anomaly": False
        }
    })


class NormalizedData(BaseModel):
    """Normalized sensor values (0-1 scale)"""
    speed_normalized: float = Field(..., ge=0, le=1, description="Normalized speed")
    rpm_normalized: float = Field(..., ge=0, le=1, description="Normalized RPM")
    temp_normalized: float = Field(..., ge=0, le=1, description="Normalized temperature")

    model_config = ConfigDict(json_schema_extra={
        "example": {
            "speed_normalized": 0.436,
            "rpm_normalized": 0.3125,
            "temp_normalized": 0.683
        }
    })


class ProcessingMetadata(BaseModel):
    """Metadata about data processing"""
    processed_at: datetime = Field(..., description="Processing timestamp")
    processor_version: str = Field(default="v1.0", description="Preprocessor version")
    enrichment_latency_ms: float = Field(..., description="Enrichment latency in milliseconds")

    model_config = ConfigDict(json_schema_extra={
        "example": {
            "processed_at": "2026-02-15T10:30:01Z",
            "processor_version": "v1.0",
            "enrichment_latency_ms": 12.5
        }
    })


class EnrichedTelemetry(BaseModel):
    """Enriched telemetry data after preprocessing"""
    # Original fields from raw telemetry
    vehicle_id: str
    timestamp: datetime
    trace_id: str
    span_id: str
    sensor_data: SensorData
    location: LocationData
    metadata: dict = Field(default_factory=dict)

    # Enrichment fields
    derived_metrics: DerivedMetrics
    anomaly_flags: AnomalyFlags
    normalization: NormalizedData
    processing_metadata: ProcessingMetadata

    model_config = ConfigDict(json_schema_extra={
        "example": {
            "vehicle_id": "VEH12345",
            "timestamp": "2026-02-15T10:30:00Z",
            "trace_id": "abc123",
            "span_id": "def456",
            "sensor_data": {
                "speed_kmh": 65.5,
                "rpm": 2500,
                "engine_temp_celsius": 90.0,
                "fuel_level_percent": 75.0,
                "throttle_position_percent": 45.0,
                "brake_pressure_bar": 0.5,
                "coolant_temp_celsius": 85.0,
                "oil_pressure_bar": 4.5,
                "battery_voltage": 13.8,
                "odometer_km": 45000.0
            },
            "location": {
                "latitude": 37.7749,
                "longitude": -122.4194,
                "altitude_m": 15.5
            },
            "metadata": {
                "sensor_version": "v1.0",
                "data_quality_score": 0.95
            },
            "derived_metrics": {
                "acceleration_ms2": 1.5,
                "fuel_efficiency_kmpl": 15.2,
                "engine_load_percent": 55.0,
                "brake_intensity": 0.1,
                "is_idling": False,
                "is_aggressive_driving": False
            },
            "anomaly_flags": {
                "overheating": False,
                "low_oil_pressure": False,
                "battery_issue": False,
                "sudden_deceleration": False,
                "fuel_anomaly": False
            },
            "normalization": {
                "speed_normalized": 0.436,
                "rpm_normalized": 0.3125,
                "temp_normalized": 0.683
            },
            "processing_metadata": {
                "processed_at": "2026-02-15T10:30:01Z",
                "processor_version": "v1.0",
                "enrichment_latency_ms": 12.5
            }
        }
    })
