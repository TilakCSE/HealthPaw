from database import engine
from models import Base, Dog, Device
from sqlalchemy.orm import Session


Base.metadata.create_all(engine)


with Session(engine) as session:

    # Create test dog
    dog = Dog(
        name="Test Dog",
        breed="Labrador Retriever",
        life_stage="Adult",
        body_weight_kg=27.5,
    )

    session.add(dog)
    session.flush()

    # Create HealthPaw device
    device = Device(
        device_id="cts_01",
        dog_id=dog.id,
        status="active",
    )

    session.add(device)
    session.commit()

    print("Test dog created.")
    print(f"Dog ID: {dog.id}")
    print(f"Device ID: {device.device_id}")