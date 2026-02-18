from sqlalchemy import create_engine
from models import Base

with open("app_conf.yml", "r") as f:
    app_config = yaml.safe_load(f.read())

ds = app_config["datastore"]
DB_URL = f'mysql+pymysql://{ds["user"]}:{ds["password"]}@{ds["hostname"]}:{ds["port"]}/{ds["db"]}'

def main() -> None:
    engine = create_engine(DB_URL)
    Base.metadata.create_all(engine)
    print("Tables created (or already exist) in readings.db")


if __name__ == "__main__":
    main()