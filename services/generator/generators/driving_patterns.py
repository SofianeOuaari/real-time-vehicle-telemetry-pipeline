"""
Driving Pattern Generator

Simulates realistic driving patterns based on time of day and random events.
"""
import random
from datetime import datetime
from typing import List


class DrivingPatternManager:
    """Manages driving pattern transitions for realistic behavior"""

    def __init__(self, seed: int = None):
        self.random = random.Random(seed if seed else random.randint(0, 1000000))
        self.current_pattern = "parked"
        self.pattern_start_time = datetime.now()
        self.pattern_duration = 0

    def get_driving_mode(self) -> str:
        """
        Get current driving mode based on pattern and time.

        Returns one of: parked, idling, urban, highway, aggressive, eco
        """
        # Check if we should transition to a new pattern
        if self._should_transition():
            self._transition_pattern()

        return self.current_pattern

    def _should_transition(self) -> bool:
        """Determine if it's time to transition to a new pattern"""
        elapsed = (datetime.now() - self.pattern_start_time).total_seconds()
        return elapsed >= self.pattern_duration

    def _transition_pattern(self) -> None:
        """Transition to a new driving pattern"""
        # Pattern transition probabilities based on current state
        transition_rules = {
            "parked": ["idling", "parked"],
            "idling": ["urban", "highway", "parked"],
            "urban": ["urban", "highway", "aggressive", "parked"],
            "highway": ["highway", "urban", "eco"],
            "aggressive": ["urban", "highway"],
            "eco": ["eco", "urban", "highway"]
        }

        # Get possible next states
        possible_next = transition_rules.get(self.current_pattern, ["urban"])

        # Choose next pattern with weights
        if self.current_pattern == "parked":
            weights = [0.7, 0.3]  # More likely to start driving
        elif self.current_pattern == "idling":
            weights = [0.4, 0.4, 0.2]  # Likely to drive
        elif self.current_pattern == "urban":
            weights = [0.5, 0.3, 0.1, 0.1]  # Mostly stay urban or go to highway
        elif self.current_pattern == "highway":
            weights = [0.6, 0.3, 0.1]  # Mostly stay on highway
        elif self.current_pattern == "aggressive":
            weights = [0.6, 0.4]  # Return to normal
        elif self.current_pattern == "eco":
            weights = [0.5, 0.3, 0.2]  # Stay eco or transition
        else:
            weights = [1.0 / len(possible_next)] * len(possible_next)

        # Select next pattern
        self.current_pattern = self.random.choices(possible_next, weights=weights)[0]

        # Set duration for new pattern (in seconds)
        duration_ranges = {
            "parked": (30, 120),  # 30s - 2min
            "idling": (10, 30),   # 10s - 30s
            "urban": (60, 180),   # 1min - 3min
            "highway": (120, 300), # 2min - 5min
            "aggressive": (20, 60), # 20s - 1min
            "eco": (90, 240)      # 1.5min - 4min
        }

        min_duration, max_duration = duration_ranges.get(self.current_pattern, (60, 120))
        self.pattern_duration = self.random.uniform(min_duration, max_duration)
        self.pattern_start_time = datetime.now()

    def get_time_of_day_influence(self) -> str:
        """
        Adjust driving patterns based on time of day.

        Returns suggested pattern based on time.
        """
        hour = datetime.now().hour

        # Morning rush hour (7-9 AM)
        if 7 <= hour < 9:
            return self.random.choice(["urban", "highway", "aggressive"])

        # Work hours (9 AM - 5 PM)
        elif 9 <= hour < 17:
            return self.random.choice(["parked", "urban", "eco"])

        # Evening rush hour (5-7 PM)
        elif 17 <= hour < 19:
            return self.random.choice(["urban", "highway", "aggressive"])

        # Evening/night (7 PM - 11 PM)
        elif 19 <= hour < 23:
            return self.random.choice(["urban", "eco", "parked"])

        # Night (11 PM - 7 AM)
        else:
            return "parked"  # Most vehicles parked at night


class FleetPatternCoordinator:
    """Coordinates patterns across a fleet of vehicles"""

    def __init__(self, num_vehicles: int, seed: int = None):
        self.num_vehicles = num_vehicles
        self.random = random.Random(seed if seed else random.randint(0, 1000000))
        self.pattern_managers = {
            f"VEH{i:04d}": DrivingPatternManager(seed=seed + i if seed else None)
            for i in range(num_vehicles)
        }

    def get_vehicle_mode(self, vehicle_id: str) -> str:
        """Get driving mode for a specific vehicle"""
        manager = self.pattern_managers.get(vehicle_id)
        if manager:
            # Apply time-of-day influence occasionally
            if self.random.random() < 0.1:  # 10% chance to use time-based suggestion
                return manager.get_time_of_day_influence()
            return manager.get_driving_mode()
        return "urban"  # Default fallback

    def get_all_active_vehicles(self) -> List[str]:
        """Get list of vehicles that are currently active (not parked)"""
        active = []
        for vehicle_id, manager in self.pattern_managers.items():
            if manager.current_pattern != "parked":
                active.append(vehicle_id)
        return active
