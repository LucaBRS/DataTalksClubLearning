from dataclasses import dataclass
import json

@dataclass
class Ride:
    pickup_location_id: int
    dropoff_location_id: int
    trip_distance: float
    total_amount: float
    pickup_datetime: str

def ride_from_row(row) -> Ride:
    return Ride(
        pickup_location_id=row["PULocationID"],
        dropoff_location_id=row["DOLocationID"],
        trip_distance=row["trip_distance"],
        total_amount=row["total_amount"],
        pickup_datetime=str(row["lpep_pickup_datetime"])  # ← different form yellow taxi!
    )

def ride_serializer(ride: Ride) -> bytes:
    ride_dict = {
        "pickup_location_id": ride.pickup_location_id,
        "dropoff_location_id": ride.dropoff_location_id,
        "trip_distance": ride.trip_distance,
        "total_amount": ride.total_amount,
        "pickup_datetime": ride.pickup_datetime
    }
    return json.dumps(ride_dict).encode("utf-8")

def ride_deserializer(data: bytes) -> Ride:
    ride_dict = json.loads(data.decode("utf-8"))
    return Ride(**ride_dict)