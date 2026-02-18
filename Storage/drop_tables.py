from sqlalchemy import create_engine
from models import Base
DB_URL = "mysql+pymysql://jas:123@localhost:3306/traffic"


def main() -> None:
    engine = create_engine(DB_URL)
    Base.metadata.drop_all(engine)
    print("Tables dropped from readings.db")


if __name__ == "__main__":
    main()