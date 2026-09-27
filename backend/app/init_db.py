from database import engine
from models import Base


print("Creating HealthPaw database tables...")

Base.metadata.create_all(engine)

print("Database tables created successfully!")