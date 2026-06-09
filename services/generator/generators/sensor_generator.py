"""
Car Sensor Data Generator

Generates realistic car sensor readings with correlations and constraints.
"""
import random
import numpy as np
from datetime import datetime
from typing import Dict
from dataclasses import dataclass, field


@dataclass
class VehicleState:
    """Tracks the state of a vehicle over time"""
    vehicle_id: str
    odometer_km: float = 0.0
    fuel_level_percent: float = 100.0
    current_speed_kmh: float = 0.0
    last_timestamp: datetime = field(default_factory=datetime.now)
    driving_mode: str = "parked"  # parked, urban, highway, aggressive, eco


class CarSensorGenerator:
    """Generates realistic car sensor data"""

    def __init__(self, vehicle_id: str, seed: int = None):
        self.vehicle_id = vehicle_id
        self.state = VehicleState(vehicle_id=vehicle_id)
        self.random = random.Random(seed if seed else random.randint(0, 1000000))
        self.np_random = np.random.RandomState(seed if seed else random.randint(0, 1000000))

    def generate_sensor_reading(self, driving_mode: str = None) -> Dict:
        """Generate a complete sensor reading based on vehicle state"""
        # Update driving mode if provided
        if driving_mode:
            self.state.driving_mode = driving_mode

        # Generate correlated sensor values based on driving mode
        sensor_data = self._generate_correlated_sensors()

        # Update vehicle state for next reading
        self._update_vehicle_state(sensor_data)

        return sensor_data

    def _generate_correlated_sensors(self) -> Dict:
        """Generate sensor values with realistic correlations"""
        mode = self.state.driving_mode

        # Base parameters by driving mode
        if mode == "parked":
            target_speed = 0.0
            target_rpm = 0
            target_throttle = 0.0
            brake_pressure = 0.0
        elif mode == "idling":
            target_speed = 0.0
            target_rpm = 850 + self.np_random.normal(0, 50)
            target_throttle = 0.0
            brake_pressure = 0.0
        elif mode == "urban":
            target_speed = 30 + self.np_random.normal(0, 10)
            target_rpm = 1800 + self.np_random.normal(0, 300)
            target_throttle = 35 + self.np_random.normal(0, 10)
            brake_pressure = self.np_random.uniform(0, 3) if self.random.random() < 0.3 else 0.0
        elif mode == "highway":
            target_speed = 110 + self.np_random.normal(0, 15)
            target_rpm = 2800 + self.np_random.normal(0, 200)
            target_throttle = 60 + self.np_random.normal(0, 8)
            brake_pressure = 0.0
        elif mode == "aggressive":
            target_speed = 80 + self.np_random.normal(0, 20)
            target_rpm = 4500 + self.np_random.normal(0, 500)
            target_throttle = 85 + self.np_random.normal(0, 10)
            brake_pressure = self.np_random.uniform(4, 8) if self.random.random() < 0.4 else 0.0
        elif mode == "eco":
            target_speed = 70 + self.np_random.normal(0, 10)
            target_rpm = 1600 + self.np_random.normal(0, 200)
            target_throttle = 40 + self.np_random.normal(0, 5)
            brake_pressure = 0.0
        else:
            target_speed = 50.0
            target_rpm = 2000
            target_throttle = 45.0
            brake_pressure = 0.0

        # Apply smooth transitions from current state
        speed_kmh = self._smooth_transition(self.state.current_speed_kmh, target_speed, 0.3)
        rpm = max(0, int(target_rpm))
        throttle_position_percent = np.clip(target_throttle, 0, 100)

        # Engine temperature correlates with RPM and duration
        base_temp = 70 + (rpm / 8000) * 30  # Higher RPM = higher temp
        engine_temp_celsius = base_temp + self.np_random.normal(0, 3)

        # Coolant temp slightly lower than engine temp
        coolant_temp_celsius = engine_temp_celsius - self.np_random.uniform(5, 10)

        # Oil pressure correlates with RPM
        if rpm > 800:
            oil_pressure_bar = 2.5 + (rpm / 8000) * 3.5 + self.np_random.normal(0, 0.3)
        else:
            oil_pressure_bar = 0.0  # Engine off

        # Battery voltage slightly drops under load
        battery_load_factor = (throttle_position_percent / 100) * 0.5
        battery_voltage = 13.8 - battery_load_factor + self.np_random.normal(0, 0.2)

        return {
            "speed_kmh": np.clip(speed_kmh, 0, 300),
            "rpm": np.clip(rpm, 0, 8000),
            "engine_temp_celsius": np.clip(engine_temp_celsius, 0, 150),
            "fuel_level_percent": np.clip(self.state.fuel_level_percent, 0, 100),
            "throttle_position_percent": np.clip(throttle_position_percent, 0, 100),
            "brake_pressure_bar": np.clip(brake_pressure, 0, 10),
            "coolant_temp_celsius": np.clip(coolant_temp_celsius, 0, 150),
            "oil_pressure_bar": np.clip(oil_pressure_bar, 0, 10),
            "battery_voltage": np.clip(battery_voltage, 11, 15),
            "odometer_km": self.state.odometer_km,
        }

    def _smooth_transition(self, current: float, target: float, factor: float = 0.3) -> float:
        """Smooth transition from current to target value"""
        return current + (target - current) * factor

    def _update_vehicle_state(self, sensor_data: Dict) -> None:
        """Update vehicle state based on sensor reading"""
        # Update odometer (assuming 1-second intervals for simplicity)
        speed_ms = sensor_data["speed_kmh"] / 3.6  # Convert km/h to m/s
        self.state.odometer_km += speed_ms / 1000  # Convert to km

        # Update fuel level (rough estimation)
        if sensor_data["speed_kmh"] > 0:
            fuel_consumption_rate = sensor_data["throttle_position_percent"] / 100 * 0.002
            self.state.fuel_level_percent -= fuel_consumption_rate

        # Update current speed
        self.state.current_speed_kmh = sensor_data["speed_kmh"]

        # Prevent negative fuel
        self.state.fuel_level_percent = max(0, self.state.fuel_level_percent)

    def generate_location(self, base_lat: float = 37.7749, base_lon: float = -122.4194) -> Dict:
        """Generate GPS location with some movement"""
        # Random walk for location
        lat_offset = self.np_random.uniform(-0.01, 0.01)
        lon_offset = self.np_random.uniform(-0.01, 0.01)

        return {
            "latitude": np.clip(base_lat + lat_offset, -90, 90),
            "longitude": np.clip(base_lon + lon_offset, -180, 180),
            "altitude_m": self.np_random.uniform(0, 100)
        }
