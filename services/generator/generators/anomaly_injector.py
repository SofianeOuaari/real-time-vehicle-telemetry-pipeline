"""
Anomaly Injector

Injects synthetic anomalies into telemetry data for testing detection capabilities.
"""
import random
from typing import Dict, Optional
from enum import Enum


class AnomalyType(Enum):
    """Types of anomalies that can be injected"""
    OVERHEATING = "overheating"
    LOW_OIL_PRESSURE = "low_oil_pressure"
    BATTERY_DRAIN = "battery_drain"
    SUDDEN_STOP = "sudden_stop"
    FUEL_LEAK = "fuel_leak"


class AnomalyInjector:
    """Injects synthetic anomalies into sensor data"""

    def __init__(self, anomaly_probability: float = 0.08, seed: int = None):
        """
        Initialize anomaly injector.

        Args:
            anomaly_probability: Probability of injecting an anomaly (0-1)
            seed: Random seed for reproducibility
        """
        self.anomaly_probability = anomaly_probability
        self.random = random.Random(seed if seed else random.randint(0, 1000000))
        self.active_anomalies: Dict[str, Dict] = {}  # vehicle_id -> anomaly info

    def should_inject_anomaly(self, vehicle_id: str) -> bool:
        """Determine if an anomaly should be injected for this vehicle"""
        # If vehicle already has an active anomaly, continue it
        if vehicle_id in self.active_anomalies:
            anomaly_info = self.active_anomalies[vehicle_id]
            anomaly_info["duration"] -= 1

            # End anomaly if duration expired
            if anomaly_info["duration"] <= 0:
                del self.active_anomalies[vehicle_id]
                return False
            return True

        # Random chance to start a new anomaly
        return self.random.random() < self.anomaly_probability

    def inject_anomaly(self, vehicle_id: str, sensor_data: Dict) -> Dict:
        """
        Inject anomaly into sensor data.

        Args:
            vehicle_id: Vehicle identifier
            sensor_data: Original sensor data

        Returns:
            Modified sensor data with injected anomaly
        """
        if vehicle_id not in self.active_anomalies:
            # Start a new anomaly
            anomaly_type = self.random.choice(list(AnomalyType))
            duration = self.random.randint(3, 10)  # Anomaly lasts 3-10 readings
            self.active_anomalies[vehicle_id] = {
                "type": anomaly_type,
                "duration": duration,
                "severity": self.random.uniform(0.5, 1.0)
            }

        anomaly_info = self.active_anomalies[vehicle_id]
        anomaly_type = anomaly_info["type"]
        severity = anomaly_info["severity"]

        # Apply anomaly based on type
        if anomaly_type == AnomalyType.OVERHEATING:
            return self._inject_overheating(sensor_data, severity)
        elif anomaly_type == AnomalyType.LOW_OIL_PRESSURE:
            return self._inject_low_oil_pressure(sensor_data, severity)
        elif anomaly_type == AnomalyType.BATTERY_DRAIN:
            return self._inject_battery_drain(sensor_data, severity)
        elif anomaly_type == AnomalyType.SUDDEN_STOP:
            return self._inject_sudden_stop(sensor_data, severity)
        elif anomaly_type == AnomalyType.FUEL_LEAK:
            return self._inject_fuel_leak(sensor_data, severity)

        return sensor_data

    def _inject_overheating(self, sensor_data: Dict, severity: float) -> Dict:
        """Inject overheating anomaly"""
        modified = sensor_data.copy()

        # Gradually increase temperature
        temp_increase = 20 + (severity * 25)  # 20-45°C increase
        modified["engine_temp_celsius"] = min(150, sensor_data["engine_temp_celsius"] + temp_increase)
        modified["coolant_temp_celsius"] = min(150, sensor_data["coolant_temp_celsius"] + temp_increase * 0.8)

        return modified

    def _inject_low_oil_pressure(self, sensor_data: Dict, severity: float) -> Dict:
        """Inject low oil pressure anomaly"""
        modified = sensor_data.copy()

        # Reduce oil pressure significantly
        pressure_drop = severity * 3.0  # Drop by up to 3 bar
        modified["oil_pressure_bar"] = max(0, sensor_data["oil_pressure_bar"] - pressure_drop)

        return modified

    def _inject_battery_drain(self, sensor_data: Dict, severity: float) -> Dict:
        """Inject battery drain anomaly"""
        modified = sensor_data.copy()

        # Reduce battery voltage
        voltage_drop = severity * 2.5  # Drop by up to 2.5V
        modified["battery_voltage"] = max(10, sensor_data["battery_voltage"] - voltage_drop)

        return modified

    def _inject_sudden_stop(self, sensor_data: Dict, severity: float) -> Dict:
        """Inject sudden stop anomaly (emergency braking)"""
        modified = sensor_data.copy()

        # Sudden speed reduction and high brake pressure
        modified["speed_kmh"] = max(0, sensor_data["speed_kmh"] * (1 - severity))
        modified["brake_pressure_bar"] = min(10, 8 + severity * 2)
        modified["throttle_position_percent"] = 0

        return modified

    def _inject_fuel_leak(self, sensor_data: Dict, severity: float) -> Dict:
        """Inject fuel leak anomaly"""
        modified = sensor_data.copy()

        # Rapid fuel level decrease
        fuel_drop = severity * 5.0  # Drop by up to 5% per reading
        modified["fuel_level_percent"] = max(0, sensor_data["fuel_level_percent"] - fuel_drop)

        return modified

    def get_active_anomaly_info(self, vehicle_id: str) -> Optional[Dict]:
        """Get information about active anomaly for a vehicle"""
        return self.active_anomalies.get(vehicle_id)

    def reset_anomaly(self, vehicle_id: str) -> None:
        """Reset/clear any active anomaly for a vehicle"""
        if vehicle_id in self.active_anomalies:
            del self.active_anomalies[vehicle_id]
