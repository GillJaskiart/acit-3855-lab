from sqlalchemy import create_engine
from models import Base

DB_URL = "mysql+pymysql://jas:123@localhost:3306/traffic"

def main() -> None:
    engine = create_engine(DB_URL)
    Base.metadata.create_all(engine)
    print("Tables created (or already exist) in readings.db")


if __name__ == "__main__":
    main()